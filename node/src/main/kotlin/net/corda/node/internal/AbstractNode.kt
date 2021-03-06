package net.corda.node.internal

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors
import com.google.common.util.concurrent.SettableFuture
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner
import net.corda.core.*
import net.corda.core.crypto.*
import net.corda.core.flows.FlowInitiator
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.StartableByRPC
import net.corda.core.identity.Party
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.RPCOps
import net.corda.core.messaging.SingleMessageRecipient
import net.corda.core.node.*
import net.corda.core.node.services.*
import net.corda.core.node.services.NetworkMapCache.MapChange
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.serialization.deserialize
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.debug
import net.corda.flows.*
import net.corda.node.services.*
import net.corda.node.services.api.*
import net.corda.node.services.config.FullNodeConfiguration
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.services.config.configureWithDevSSLCertificate
import net.corda.node.services.events.NodeSchedulerService
import net.corda.node.services.events.ScheduledActivityObserver
import net.corda.node.services.identity.InMemoryIdentityService
import net.corda.node.services.keys.PersistentKeyManagementService
import net.corda.node.services.messaging.MessagingService
import net.corda.node.services.messaging.sendRequest
import net.corda.node.services.network.InMemoryNetworkMapCache
import net.corda.node.services.network.NetworkMapService
import net.corda.node.services.network.NetworkMapService.RegistrationResponse
import net.corda.node.services.network.NodeRegistration
import net.corda.node.services.network.PersistentNetworkMapService
import net.corda.node.services.persistence.*
import net.corda.node.services.schema.HibernateObserver
import net.corda.node.services.schema.NodeSchemaService
import net.corda.node.services.statemachine.FlowStateMachineImpl
import net.corda.node.services.statemachine.StateMachineManager
import net.corda.node.services.statemachine.flowVersion
import net.corda.node.services.transactions.*
import net.corda.node.services.vault.CashBalanceAsMetricsObserver
import net.corda.node.services.vault.NodeVaultService
import net.corda.node.services.vault.VaultSoftLockManager
import net.corda.node.utilities.AddOrRemove.ADD
import net.corda.node.utilities.AffinityExecutor
import net.corda.node.utilities.configureDatabase
import net.corda.node.utilities.transaction
import org.apache.activemq.artemis.utils.ReusableLatch
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.cert.X509CertificateHolder
import org.jetbrains.exposed.sql.Database
import org.slf4j.Logger
import java.io.IOException
import java.lang.reflect.Modifier.*
import java.net.URL
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Path
import java.nio.file.Paths
import java.security.KeyPair
import java.security.KeyStoreException
import java.time.Clock
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit.SECONDS
import kotlin.collections.ArrayList
import kotlin.reflect.KClass
import net.corda.core.crypto.generateKeyPair as cryptoGenerateKeyPair

/**
 * A base node implementation that can be customised either for production (with real implementations that do real
 * I/O), or a mock implementation suitable for unit test environments.
 *
 * Marked as SingletonSerializeAsToken to prevent the invisible reference to AbstractNode in the ServiceHub accidentally
 * sweeping up the Node into the Kryo checkpoint serialization via any flows holding a reference to ServiceHub.
 */
// TODO: Where this node is the initial network map service, currently no networkMapService is provided.
// In theory the NodeInfo for the node should be passed in, instead, however currently this is constructed by the
// AbstractNode. It should be possible to generate the NodeInfo outside of AbstractNode, so it can be passed in.
abstract class AbstractNode(open val configuration: NodeConfiguration,
                            val advertisedServices: Set<ServiceInfo>,
                            val platformClock: Clock,
                            @VisibleForTesting val busyNodeLatch: ReusableLatch = ReusableLatch()) : SingletonSerializeAsToken() {

    // TODO: Persist this, as well as whether the node is registered.
    /**
     * Sequence number of changes sent to the network map service, when registering/de-registering this node.
     */
    var networkMapSeq: Long = 1

    protected abstract val log: Logger
    protected abstract val networkMapAddress: SingleMessageRecipient?
    protected abstract val platformVersion: Int

    // We will run as much stuff in this single thread as possible to keep the risk of thread safety bugs low during the
    // low-performance prototyping period.
    protected abstract val serverThread: AffinityExecutor

    protected val serviceFlowFactories = ConcurrentHashMap<Class<*>, ServiceFlowInfo>()
    protected val partyKeys = mutableSetOf<KeyPair>()

    val services = object : ServiceHubInternal() {
        override val networkService: MessagingService get() = net
        override val networkMapCache: NetworkMapCacheInternal get() = netMapCache
        override val storageService: TxWritableStorageService get() = storage
        override val vaultService: VaultService get() = vault
        override val keyManagementService: KeyManagementService get() = keyManagement
        override val identityService: IdentityService get() = identity
        override val schedulerService: SchedulerService get() = scheduler
        override val clock: Clock get() = platformClock
        override val myInfo: NodeInfo get() = info
        override val schemaService: SchemaService get() = schemas
        override val transactionVerifierService: TransactionVerifierService get() = txVerifierService
        override val auditService: AuditService get() = auditService
        override val rpcFlows: List<Class<out FlowLogic<*>>> get() = this@AbstractNode.rpcFlows

        // Internal only
        override val monitoringService: MonitoringService = MonitoringService(MetricRegistry())

        override fun <T> startFlow(logic: FlowLogic<T>, flowInitiator: FlowInitiator): FlowStateMachineImpl<T> {
            return serverThread.fetchFrom { smm.add(logic, flowInitiator) }
        }

        override fun registerServiceFlow(initiatingFlowClass: Class<out FlowLogic<*>>, serviceFlowFactory: (Party) -> FlowLogic<*>) {
            require(initiatingFlowClass !in serviceFlowFactories) {
                "${initiatingFlowClass.name} has already been used to register a service flow"
            }
            val info = ServiceFlowInfo.CorDapp(initiatingFlowClass.flowVersion, serviceFlowFactory)
            log.info("Registering service flow for ${initiatingFlowClass.name}: $info")
            serviceFlowFactories[initiatingFlowClass] = info
        }

        override fun getServiceFlowFactory(clientFlowClass: Class<out FlowLogic<*>>): ServiceFlowInfo? {
            return serviceFlowFactories[clientFlowClass]
        }

        override fun recordTransactions(txs: Iterable<SignedTransaction>) {
            database.transaction {
                recordTransactionsInternal(storage, txs)
            }
        }
    }

    open fun findMyLocation(): PhysicalLocation? = CityDatabase[configuration.nearestCity]

    lateinit var info: NodeInfo
    lateinit var storage: TxWritableStorageService
    lateinit var checkpointStorage: CheckpointStorage
    lateinit var smm: StateMachineManager
    lateinit var vault: VaultService
    lateinit var keyManagement: KeyManagementService
    var inNodeNetworkMapService: NetworkMapService? = null
    lateinit var txVerifierService: TransactionVerifierService
    lateinit var identity: IdentityService
    lateinit var net: MessagingService
    lateinit var netMapCache: NetworkMapCacheInternal
    lateinit var scheduler: NodeSchedulerService
    lateinit var schemas: SchemaService
    lateinit var auditService: AuditService
    val customServices: ArrayList<Any> = ArrayList()
    protected val runOnStop: ArrayList<Runnable> = ArrayList()
    lateinit var database: Database
    protected var dbCloser: Runnable? = null
    private lateinit var rpcFlows: List<Class<out FlowLogic<*>>>

    /** Locates and returns a service of the given type if loaded, or throws an exception if not found. */
    inline fun <reified T : Any> findService() = customServices.filterIsInstance<T>().single()

    var isPreviousCheckpointsPresent = false
        private set

    protected val _networkMapRegistrationFuture: SettableFuture<Unit> = SettableFuture.create()
    /** Completes once the node has successfully registered with the network map service */
    val networkMapRegistrationFuture: ListenableFuture<Unit>
        get() = _networkMapRegistrationFuture

    /** Fetch CordaPluginRegistry classes registered in META-INF/services/net.corda.core.node.CordaPluginRegistry files that exist in the classpath */
    open val pluginRegistries: List<CordaPluginRegistry> by lazy {
        ServiceLoader.load(CordaPluginRegistry::class.java).toList()
    }

    /** Set to true once [start] has been successfully called. */
    @Volatile var started = false
        private set

    /** The implementation of the [CordaRPCOps] interface used by this node. */
    open val rpcOps: CordaRPCOps by lazy { CordaRPCOpsImpl(services, smm, database) }   // Lazy to avoid init ordering issue with the SMM.

    open fun start(): AbstractNode {
        require(!started) { "Node has already been started" }

        if (configuration.devMode) {
            log.warn("Corda node is running in dev mode.")
            configuration.configureWithDevSSLCertificate()
        }
        require(hasSSLCertificates()) {
            "Identity certificate not found. " +
                    "Please either copy your existing identity key and certificate from another node, " +
                    "or if you don't have one yet, fill out the config file and run corda.jar --initial-registration. " +
                    "Read more at: https://docs.corda.net/permissioning.html"
        }

        log.info("Node starting up ...")

        // Do all of this in a database transaction so anything that might need a connection has one.
        initialiseDatabasePersistence {
            val tokenizableServices = makeServices()

            smm = StateMachineManager(services,
                    listOf(tokenizableServices),
                    checkpointStorage,
                    serverThread,
                    database,
                    busyNodeLatch)
            if (serverThread is ExecutorService) {
                runOnStop += Runnable {
                    // We wait here, even though any in-flight messages should have been drained away because the
                    // server thread can potentially have other non-messaging tasks scheduled onto it. The timeout value is
                    // arbitrary and might be inappropriate.
                    MoreExecutors.shutdownAndAwaitTermination(serverThread as ExecutorService, 50, SECONDS)
                }
            }

            makeVaultObservers()

            checkpointStorage.forEach {
                isPreviousCheckpointsPresent = true
                false
            }
            startMessagingService(rpcOps)
            installCoreFlows()

            fun Class<out FlowLogic<*>>.isUserInvokable(): Boolean {
                return isPublic(modifiers) && !isLocalClass && !isAnonymousClass && (!isMemberClass || isStatic(modifiers))
            }

            val flows = scanForFlows()
            rpcFlows = flows.filter { it.isUserInvokable() && it.isAnnotationPresent(StartableByRPC::class.java) } +
                    // Add any core flows here
                    listOf(ContractUpgradeFlow::class.java)

            runOnStop += Runnable { net.stop() }
            _networkMapRegistrationFuture.setFuture(registerWithNetworkMapIfConfigured())
            smm.start()
            // Shut down the SMM so no Fibers are scheduled.
            runOnStop += Runnable { smm.stop(acceptableLiveFiberCountOnStop()) }
            scheduler.start()
        }
        started = true
        return this
    }

    /**
     * Installs a flow that's core to the Corda platform. Unlike CorDapp flows which are versioned individually using
     * [InitiatingFlow.version], core flows have the same version as the node's platform version. To cater for backwards
     * compatibility [serviceFlowFactory] provides a second parameter which is the platform version of the initiating party.
     * @suppress
     */
    @VisibleForTesting
    fun installCoreFlow(clientFlowClass: KClass<out FlowLogic<*>>, serviceFlowFactory: (Party, Int) -> FlowLogic<*>) {
        require(clientFlowClass.java.flowVersion == 1) {
            "${InitiatingFlow::class.java.name}.version not applicable for core flows; their version is the node's platform version"
        }
        serviceFlowFactories[clientFlowClass.java] = ServiceFlowInfo.Core(serviceFlowFactory)
        log.debug { "Installed core flow ${clientFlowClass.java.name}" }
    }

    private fun installCoreFlows() {
        installCoreFlow(FetchTransactionsFlow::class) { otherParty, _ -> FetchTransactionsHandler(otherParty) }
        installCoreFlow(FetchAttachmentsFlow::class) { otherParty, _ -> FetchAttachmentsHandler(otherParty) }
        installCoreFlow(BroadcastTransactionFlow::class) { otherParty, _ -> NotifyTransactionHandler(otherParty) }
        installCoreFlow(NotaryChangeFlow::class) { otherParty, _ -> NotaryChangeHandler(otherParty) }
        installCoreFlow(ContractUpgradeFlow::class) { otherParty, _ -> ContractUpgradeHandler(otherParty) }
    }

    /**
     * Builds node internal, advertised, and plugin services.
     * Returns a list of tokenizable services to be added to the serialisation context.
     */
    private fun makeServices(): MutableList<Any> {
        val storageServices = initialiseStorageService(configuration.baseDirectory)
        storage = storageServices.first
        checkpointStorage = storageServices.second
        netMapCache = InMemoryNetworkMapCache()
        net = makeMessagingService()
        schemas = makeSchemaService()
        vault = makeVaultService(configuration.dataSourceProperties)
        txVerifierService = makeTransactionVerifierService()
        auditService = DummyAuditService()

        info = makeInfo()
        identity = makeIdentityService()
        // Place the long term identity key in the KMS. Eventually, this is likely going to be separated again because
        // the KMS is meant for derived temporary keys used in transactions, and we're not supposed to sign things with
        // the identity key. But the infrastructure to make that easy isn't here yet.
        keyManagement = makeKeyManagementService()
        scheduler = NodeSchedulerService(services, database, unfinishedSchedules = busyNodeLatch)

        val tokenizableServices = mutableListOf(storage, net, vault, keyManagement, identity, platformClock, scheduler)
        makeAdvertisedServices(tokenizableServices)

        customServices.clear()
        customServices.addAll(makePluginServices(tokenizableServices))

        initUploaders(storageServices)
        return tokenizableServices
    }

    private fun scanForFlows(): List<Class<out FlowLogic<*>>> {
        val pluginsDir = configuration.baseDirectory / "plugins"
        log.info("Scanning plugins in $pluginsDir ...")
        if (!pluginsDir.exists()) return emptyList()

        val pluginJars = pluginsDir.list {
            it.filter { it.isRegularFile() && it.toString().endsWith(".jar") }.toArray()
        }

        if (pluginJars.isEmpty()) return emptyList()

        val scanResult = FastClasspathScanner().overrideClasspath(*pluginJars).scan()  // This will only scan the plugin jars and nothing else

        fun loadFlowClass(className: String): Class<out FlowLogic<*>>? {
            return try {
                // TODO Make sure this is loaded by the correct class loader
                @Suppress("UNCHECKED_CAST")
                Class.forName(className, false, javaClass.classLoader) as Class<out FlowLogic<*>>
            } catch (e: Exception) {
                log.warn("Unable to load flow class $className", e)
                null
            }
        }

        val flowClasses = scanResult.getNamesOfSubclassesOf(FlowLogic::class.java)
                .mapNotNull { loadFlowClass(it) }
                .filterNot { isAbstract(it.modifiers) }

        fun URL.pluginName(): String {
            return try {
                Paths.get(toURI()).fileName.toString()
            } catch (e: Exception) {
                toString()
            }
        }

        flowClasses.groupBy {
            scanResult.classNameToClassInfo[it.name]!!.classpathElementURLs.first()
        }.forEach { url, classes ->
            log.info("Found flows in plugin ${url.pluginName()}: ${classes.joinToString { it.name }}")
        }

        return flowClasses
    }

    private fun initUploaders(storageServices: Pair<TxWritableStorageService, CheckpointStorage>) {
        val uploaders: List<FileUploader> = listOf(storageServices.first.attachments as NodeAttachmentService) +
                customServices.filterIsInstance(AcceptsFileUpload::class.java)
        (storage as StorageServiceImpl).initUploaders(uploaders)
    }

    private fun makeVaultObservers() {
        VaultSoftLockManager(vault, smm)
        CashBalanceAsMetricsObserver(services, database)
        ScheduledActivityObserver(services)
        HibernateObserver(vault.rawUpdates, schemas)
    }

    private fun makeInfo(): NodeInfo {
        val advertisedServiceEntries = makeServiceEntries()
        val legalIdentity = obtainLegalIdentity()
        return NodeInfo(net.myAddress, legalIdentity, platformVersion, advertisedServiceEntries, findMyLocation())
    }

    /**
     * A service entry contains the advertised [ServiceInfo] along with the service identity. The identity *name* is
     * taken from the configuration or, if non specified, generated by combining the node's legal name and the service id.
     */
    protected open fun makeServiceEntries(): List<ServiceEntry> {
        return advertisedServices.map {
            val serviceId = it.type.id
            val serviceName = it.name ?: configuration.myLegalName.replaceCommonName(serviceId)
            val identity = obtainKeyPair(serviceId, serviceName).first
            ServiceEntry(it, identity)
        }
    }

    @VisibleForTesting
    protected open fun acceptableLiveFiberCountOnStop(): Int = 0

    private fun hasSSLCertificates(): Boolean {
        val keyStore = try {
            // This will throw IOException if key file not found or KeyStoreException if keystore password is incorrect.
            KeyStoreUtilities.loadKeyStore(configuration.keyStoreFile, configuration.keyStorePassword)
        } catch (e: IOException) {
            null
        } catch (e: KeyStoreException) {
            log.warn("Certificate key store found but key store password does not match configuration.")
            null
        }
        return keyStore?.containsAlias(X509Utilities.CORDA_CLIENT_CA) ?: false
    }

    // Specific class so that MockNode can catch it.
    class DatabaseConfigurationException(msg: String) : Exception(msg)

    protected open fun initialiseDatabasePersistence(insideTransaction: () -> Unit) {
        val props = configuration.dataSourceProperties
        if (props.isNotEmpty()) {
            val (toClose, database) = configureDatabase(props)
            this.database = database
            // Now log the vendor string as this will also cause a connection to be tested eagerly.
            log.info("Connected to ${database.vendor} database.")
            dbCloser = Runnable { toClose.close() }
            runOnStop += dbCloser!!
            database.transaction {
                insideTransaction()
            }
        } else {
            throw DatabaseConfigurationException("There must be a database configured.")
        }
    }

    private fun makePluginServices(tokenizableServices: MutableList<Any>): List<Any> {
        val pluginServices = pluginRegistries.flatMap { it.servicePlugins }.map { it.apply(services) }
        tokenizableServices.addAll(pluginServices)
        return pluginServices
    }

    /**
     * Run any tasks that are needed to ensure the node is in a correct state before running start().
     */
    open fun setup(): AbstractNode {
        createNodeDir()
        return this
    }

    private fun makeAdvertisedServices(tokenizableServices: MutableList<Any>) {
        val serviceTypes = info.advertisedServices.map { it.info.type }
        if (NetworkMapService.type in serviceTypes) makeNetworkMapService()

        val notaryServiceType = serviceTypes.singleOrNull { it.isNotary() }
        if (notaryServiceType != null) {
            makeNotaryService(notaryServiceType, tokenizableServices)
        }
    }

    private fun registerWithNetworkMapIfConfigured(): ListenableFuture<Unit> {
        services.networkMapCache.addNode(info)
        // In the unit test environment, we may sometimes run without any network map service
        return if (networkMapAddress == null && inNodeNetworkMapService == null) {
            services.networkMapCache.runWithoutMapService()
            noNetworkMapConfigured()  // TODO This method isn't needed as runWithoutMapService sets the Future in the cache
        } else {
            registerWithNetworkMap()
        }
    }

    /**
     * Register this node with the network map cache, and load network map from a remote service (and register for
     * updates) if one has been supplied.
     */
    protected open fun registerWithNetworkMap(): ListenableFuture<Unit> {
        require(networkMapAddress != null || NetworkMapService.type in advertisedServices.map { it.type }) {
            "Initial network map address must indicate a node that provides a network map service"
        }
        val address = networkMapAddress ?: info.address
        // Register for updates, even if we're the one running the network map.
        return sendNetworkMapRegistration(address).flatMap { (error) ->
            check(error == null) { "Unable to register with the network map service: $error" }
            // The future returned addMapService will complete on the same executor as sendNetworkMapRegistration, namely the one used by net
            services.networkMapCache.addMapService(net, address, true, null)
        }
    }

    private fun sendNetworkMapRegistration(networkMapAddress: SingleMessageRecipient): ListenableFuture<RegistrationResponse> {
        // Register this node against the network
        val instant = platformClock.instant()
        val expires = instant + NetworkMapService.DEFAULT_EXPIRATION_PERIOD
        val reg = NodeRegistration(info, instant.toEpochMilli(), ADD, expires)
        val legalIdentityKey = obtainLegalIdentityKey()
        val request = NetworkMapService.RegistrationRequest(reg.toWire(legalIdentityKey.private), net.myAddress)
        return net.sendRequest(NetworkMapService.REGISTER_TOPIC, request, networkMapAddress)
    }

    /** This is overriden by the mock node implementation to enable operation without any network map service */
    protected open fun noNetworkMapConfigured(): ListenableFuture<Unit> {
        // TODO: There should be a consistent approach to configuration error exceptions.
        throw IllegalStateException("Configuration error: this node isn't being asked to act as the network map, nor " +
                "has any other map node been configured.")
    }

    protected open fun makeKeyManagementService(): KeyManagementService = PersistentKeyManagementService(partyKeys)

    open protected fun makeNetworkMapService() {
        inNodeNetworkMapService = PersistentNetworkMapService(services, configuration.minimumPlatformVersion)
    }

    open protected fun makeNotaryService(type: ServiceType, tokenizableServices: MutableList<Any>) {
        val timestampChecker = TimestampChecker(platformClock, 30.seconds)
        val uniquenessProvider = makeUniquenessProvider(type)
        tokenizableServices.add(uniquenessProvider)

        val notaryService = when (type) {
            SimpleNotaryService.type -> SimpleNotaryService(timestampChecker, uniquenessProvider)
            ValidatingNotaryService.type -> ValidatingNotaryService(timestampChecker, uniquenessProvider)
            RaftNonValidatingNotaryService.type -> RaftNonValidatingNotaryService(timestampChecker, uniquenessProvider as RaftUniquenessProvider)
            RaftValidatingNotaryService.type -> RaftValidatingNotaryService(timestampChecker, uniquenessProvider as RaftUniquenessProvider)
            BFTNonValidatingNotaryService.type -> with(configuration as FullNodeConfiguration) {
                val nodeId = notaryNodeId ?: throw IllegalArgumentException("notaryNodeId value must be specified in the configuration")
                val client = BFTSMaRt.Client(nodeId)
                tokenizableServices += client
                BFTNonValidatingNotaryService(services, timestampChecker, nodeId, database, client)
            }
            else -> {
                throw IllegalArgumentException("Notary type ${type.id} is not handled by makeNotaryService.")
            }
        }

        installCoreFlow(NotaryFlow.Client::class, notaryService.serviceFlowFactory)
    }

    protected abstract fun makeUniquenessProvider(type: ServiceType): UniquenessProvider

    protected open fun makeIdentityService(): IdentityService {
        val service = InMemoryIdentityService()
        service.registerIdentity(info.legalIdentity)
        services.networkMapCache.partyNodes.forEach { service.registerIdentity(it.legalIdentity) }
        netMapCache.changed.subscribe { mapChange ->
            // TODO how should we handle network map removal
            if (mapChange is MapChange.Added) {
                service.registerIdentity(mapChange.node.legalIdentity)
            }
        }
        return service
    }

    // TODO: sort out ordering of open & protected modifiers of functions in this class.
    protected open fun makeVaultService(dataSourceProperties: Properties): VaultService = NodeVaultService(services, dataSourceProperties)

    protected open fun makeSchemaService(): SchemaService = NodeSchemaService()

    protected abstract fun makeTransactionVerifierService(): TransactionVerifierService

    open fun stop() {
        // TODO: We need a good way of handling "nice to have" shutdown events, especially those that deal with the
        // network, including unsubscribing from updates from remote services. Possibly some sort of parameter to stop()
        // to indicate "Please shut down gracefully" vs "Shut down now".
        // Meanwhile, we let the remote service send us updates until the acknowledgment buffer overflows and it
        // unsubscribes us forcibly, rather than blocking the shutdown process.

        // Run shutdown hooks in opposite order to starting
        for (toRun in runOnStop.reversed()) {
            toRun.run()
        }
        runOnStop.clear()
    }

    protected abstract fun makeMessagingService(): MessagingService

    protected abstract fun startMessagingService(rpcOps: RPCOps)

    protected open fun initialiseStorageService(dir: Path): Pair<TxWritableStorageService, CheckpointStorage> {
        val attachments = makeAttachmentStorage(dir)
        val checkpointStorage = DBCheckpointStorage()
        val transactionStorage = DBTransactionStorage()
        val stateMachineTransactionMappingStorage = DBTransactionMappingStorage()
        return Pair(
                constructStorageService(attachments, transactionStorage, stateMachineTransactionMappingStorage),
                checkpointStorage
        )
    }

    protected open fun constructStorageService(attachments: AttachmentStorage,
                                               transactionStorage: TransactionStorage,
                                               stateMachineRecordedTransactionMappingStorage: StateMachineRecordedTransactionMappingStorage) =
            StorageServiceImpl(attachments, transactionStorage, stateMachineRecordedTransactionMappingStorage)

    protected fun obtainLegalIdentity(): Party = obtainKeyPair().first
    protected fun obtainLegalIdentityKey(): KeyPair = obtainKeyPair().second

    private fun obtainKeyPair(serviceId: String = "identity", serviceName: X500Name = configuration.myLegalName): Pair<Party, KeyPair> {
        // Load the private identity key, creating it if necessary. The identity key is a long term well known key that
        // is distributed to other peers and we use it (or a key signed by it) when we need to do something
        // "permissioned". The identity file is what gets distributed and contains the node's legal name along with
        // the public key. Obviously in a real system this would need to be a certificate chain of some kind to ensure
        // the legal name is actually validated in some way.

        // TODO: Integrate with Key management service?
        val keystore = KeyStoreUtilities.loadKeyStore(configuration.keyStoreFile, configuration.keyStorePassword)
        val privateKeyAlias = "$serviceId-private-key"
        val privKeyFile = configuration.baseDirectory / privateKeyAlias
        val pubIdentityFile = configuration.baseDirectory / "$serviceId-public"

        val identityAndKey = if (configuration.keyStoreFile.exists() && keystore.containsAlias(privateKeyAlias)) {
            // Get keys from keystore.
            val (cert, keyPair) = keystore.getCertificateAndKey(privateKeyAlias, configuration.keyStorePassword)
            val loadedServiceName = X509CertificateHolder(cert.encoded).subject
            if (X509CertificateHolder(cert.encoded).subject != serviceName) {
                throw ConfigurationException("The legal name in the config file doesn't match the stored identity keystore:" +
                        "$serviceName vs $loadedServiceName")
            }
            Pair(Party(loadedServiceName, keyPair.public), keyPair)
        } else if (privKeyFile.exists()) {
            // Get keys from key file.
            // TODO: this is here to smooth out the key storage transition, remove this in future release.
            // Check that the identity in the config file matches the identity file we have stored to disk.
            // This is just a sanity check. It shouldn't fail unless the admin has fiddled with the files and messed
            // things up for us.
            val myIdentity = pubIdentityFile.readAll().deserialize<Party>()
            if (myIdentity.name != serviceName)
                throw ConfigurationException("The legal name in the config file doesn't match the stored identity file:" +
                        "$serviceName vs ${myIdentity.name}")
            // Load the private key.
            val keyPair = privKeyFile.readAll().deserialize<KeyPair>()
            // TODO: Use a proper certificate chain.
            val selfSignCert = X509Utilities.createSelfSignedCACert(serviceName, keyPair)
            keystore.addOrReplaceKey(privateKeyAlias, keyPair.private, configuration.keyStorePassword.toCharArray(), arrayOf(selfSignCert.certificate))
            keystore.save(configuration.keyStoreFile, configuration.keyStorePassword)
            Pair(myIdentity, keyPair)
        } else {
            // Create new keys and store in keystore.
            log.info("Identity key not found, generating fresh key!")
            val keyPair: KeyPair = generateKeyPair()
            val selfSignCert = X509Utilities.createSelfSignedCACert(serviceName, keyPair)
            keystore.addOrReplaceKey(privateKeyAlias, selfSignCert.keyPair.private, configuration.keyStorePassword.toCharArray(), arrayOf(selfSignCert.certificate))
            keystore.save(configuration.keyStoreFile, configuration.keyStorePassword)
            Pair(Party(serviceName, selfSignCert.keyPair.public), selfSignCert.keyPair)
        }
        partyKeys += identityAndKey.second
        return identityAndKey
    }

    protected open fun generateKeyPair() = cryptoGenerateKeyPair()

    protected fun makeAttachmentStorage(dir: Path): AttachmentStorage {
        val attachmentsDir = dir / "attachments"
        try {
            attachmentsDir.createDirectory()
        } catch (e: FileAlreadyExistsException) {
        }
        return NodeAttachmentService(attachmentsDir, configuration.dataSourceProperties, services.monitoringService.metrics)
    }

    protected fun createNodeDir() {
        configuration.baseDirectory.createDirectories()
    }
}

sealed class ServiceFlowInfo {
    data class Core(val factory: (Party, Int) -> FlowLogic<*>) : ServiceFlowInfo()
    data class CorDapp(val version: Int, val factory: (Party) -> FlowLogic<*>) : ServiceFlowInfo()
}
