package net.corda.node.services

import com.google.common.util.concurrent.ListenableFuture
import net.corda.core.getOrThrow
import net.corda.core.map
import net.corda.core.messaging.send
import net.corda.core.node.NodeInfo
import net.corda.core.node.services.DEFAULT_SESSION_ID
import net.corda.flows.sendRequest
import net.corda.node.services.network.AbstractNetworkMapService
import net.corda.node.services.network.NetworkMapService
import net.corda.node.services.network.NetworkMapService.*
import net.corda.node.services.network.NetworkMapService.Companion.FETCH_TOPIC
import net.corda.node.services.network.NetworkMapService.Companion.PUSH_ACK_TOPIC
import net.corda.node.services.network.NetworkMapService.Companion.REGISTER_TOPIC
import net.corda.node.services.network.NetworkMapService.Companion.SUBSCRIPTION_TOPIC
import net.corda.node.services.network.NodeRegistration
import net.corda.node.utilities.AddOrRemove
import net.corda.node.utilities.AddOrRemove.ADD
import net.corda.node.utilities.AddOrRemove.REMOVE
import net.corda.node.utilities.databaseTransaction
import net.corda.testing.node.MockNetwork
import net.corda.testing.node.MockNetwork.MockNode
import org.assertj.core.api.Assertions.assertThat
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.security.PrivateKey
import java.time.Instant
import java.util.concurrent.Future
import kotlin.test.assertEquals
import kotlin.test.assertNull

abstract class AbstractNetworkMapServiceTest<out S : AbstractNetworkMapService> {
    lateinit var network: MockNetwork
    lateinit var mapServiceNode: MockNode
    lateinit var registerNode: MockNode

    @Before
    fun setup() {
        network = MockNetwork(defaultFactory = nodeFactory)
        network.createTwoNodes().apply {
            mapServiceNode = first
            registerNode = second
        }
    }

    @After
    fun tearDown() {
        network.stopNodes()
    }

    protected abstract val nodeFactory: MockNetwork.Factory

    protected abstract val networkMapService: S

    // For persistent service, switch out the implementation for a newly instantiated one so we can check the state is preserved.
    protected abstract fun swizzle()

    @Test
    fun `empty service`() {
        // Confirm the service contains no nodes as own node only registered if network is run.
        assertThat(networkMapService.nodes).isEmpty()
        assertThat(identityQuery()).isNull()
    }

    @Test
    fun `register new node`() {
        databaseTransaction(mapServiceNode.database) {
            val registerRequest = nodeRegistration(ADD)
            assertThat(processRegistrationRequest(registerRequest).error).isNull()
            assertThat(networkMapService.nodes).containsOnly(registerNode.info)
            swizzle()
            assertThat(networkMapService.nodes).containsOnly(registerNode.info)
            assertThat(identityQuery()).isEqualTo(registerNode.info)
        }
    }

    @Test
    fun `re-register the same node`() {
        databaseTransaction(mapServiceNode.database) {
            val registerRequest = nodeRegistration(ADD)
            processRegistrationRequest(registerRequest)
            swizzle()
            assertThat(processRegistrationRequest(registerRequest).error).isNull()
            assertThat(networkMapService.nodes).containsOnly(registerNode.info)  // Confirm it's a no-op
        }
    }

    @Test
    fun `de-register node`() {
        databaseTransaction(mapServiceNode.database) {
            val registerRequest = nodeRegistration(ADD)
            processRegistrationRequest(registerRequest)
            swizzle()
            val deRegisterRequest = registerRequest.copy(serial = registerRequest.serial.inc(), type = REMOVE)
            assertThat(processRegistrationRequest(deRegisterRequest).error).isNull()
            swizzle()
            // Confirm that de-registering the node succeeds and drops it from the node lists
            assertThat(networkMapService.nodes).isEmpty()
            assertThat(identityQuery()).isNull()
        }
    }

    @Test
    fun `de-register unknown node`() {
        assertThat(processRegistrationRequest(nodeRegistration(REMOVE)).error).isNotNull()
        assertThat(networkMapService.nodes).isEmpty()
    }

    @Test
    fun `de-register same node again`() {
        databaseTransaction(mapServiceNode.database) {
            val registerRequest = nodeRegistration(ADD)
            processRegistrationRequest(registerRequest)
            val deRegisterRequest = registerRequest.copy(serial = registerRequest.serial.inc(), type = REMOVE)
            processRegistrationRequest(deRegisterRequest)
            swizzle()
            assertThat(processRegistrationRequest(deRegisterRequest).error).isNotNull()
            assertThat(networkMapService.nodes).isEmpty()
            assertThat(identityQuery()).isNull()
        }
    }

    @Test
    fun `success with network`() {
        databaseTransaction(mapServiceNode.database) {
            swizzle()

            // Confirm all nodes have registered themselves
            network.runNetwork()
            var fetchResult = registerNode.fetchMap(mapServiceNode, false)
            network.runNetwork()
            assertEquals(2, fetchResult.getOrThrow()?.count())

            // Forcibly de-register the second node
            val nodeKey = registerNode.services.legalIdentityKey
            val instant = Instant.now()
            val expires = instant + NetworkMapService.DEFAULT_EXPIRATION_PERIOD
            val reg = NodeRegistration(registerNode.info, instant.toEpochMilli() + 1, AddOrRemove.REMOVE, expires)
            val registerResult = registerNode.registration(mapServiceNode, reg, nodeKey.private)
            network.runNetwork()
            assertThat(registerResult.getOrThrow().error).isNull()

            swizzle()

            // Now only map service node should be registered
            fetchResult = registerNode.fetchMap(mapServiceNode, false)
            network.runNetwork()
            assertEquals(mapServiceNode.info, fetchResult.getOrThrow()?.filter { it.type == ADD }?.map { it.node }?.single())
        }
    }

    @Test
    fun `subscribe with network 2`() {
        databaseTransaction(mapServiceNode.database) {
            swizzle()

            // Test subscribing to updates
            network.runNetwork()
            val subscribeResult = registerNode.subscribe(mapServiceNode, true)
            network.runNetwork()
            subscribeResult.getOrThrow()

            swizzle()

            val startingMapVersion = networkMapService.mapVersion

            // Check the unacknowledged count is zero
            assertEquals(0, networkMapService.getUnacknowledgedCount(registerNode.info.address, startingMapVersion))

            // Fire off an update
            val nodeKey = registerNode.services.legalIdentityKey
            var seq = 0L
            val expires = Instant.now() + NetworkMapService.DEFAULT_EXPIRATION_PERIOD
            var reg = NodeRegistration(registerNode.info, seq++, ADD, expires)
            var wireReg = reg.toWire(nodeKey.private)
            networkMapService.notifySubscribers(wireReg, startingMapVersion + 1)

            swizzle()

            // Check the unacknowledged count is one
            assertEquals(1, networkMapService.getUnacknowledgedCount(registerNode.info.address, startingMapVersion + 1))

            // Send in an acknowledgment and verify the count goes down
            registerNode.updateAcknowlege(mapServiceNode, startingMapVersion + 1)
            network.runNetwork()

            swizzle()

            assertEquals(0, networkMapService.getUnacknowledgedCount(registerNode.info.address, startingMapVersion + 1))

            // Intentionally fill the pending acknowledgements to verify it doesn't drop subscribers before the limit
            // is hit. On the last iteration overflow the pending list, and check the node is unsubscribed
            for (i in 0..networkMapService.maxUnacknowledgedUpdates) {
                reg = NodeRegistration(registerNode.info, seq++, ADD, expires)
                wireReg = reg.toWire(nodeKey.private)
                networkMapService.notifySubscribers(wireReg, i + startingMapVersion + 2)

                swizzle()

                if (i < networkMapService.maxUnacknowledgedUpdates) {
                    assertEquals(i + 1, networkMapService.getUnacknowledgedCount(registerNode.info.address, i + startingMapVersion + 2))
                } else {
                    assertNull(networkMapService.getUnacknowledgedCount(registerNode.info.address, i + startingMapVersion + 2))
                }
            }
        }
    }

    private fun identityQuery(): NodeInfo? {
        val identityRequest = QueryIdentityRequest(registerNode.info.legalIdentity, mapServiceNode.info.address)
        return networkMapService.processQueryRequest(identityRequest).node
    }

    private fun nodeRegistration(addOrRemove: AddOrRemove): NodeRegistration {
        val instant = Instant.now()
        val expires = instant + NetworkMapService.DEFAULT_EXPIRATION_PERIOD
        return NodeRegistration(registerNode.info, instant.toEpochMilli(), addOrRemove, expires)
    }

    private fun processRegistrationRequest(nodeRegistration: NodeRegistration): RegistrationResponse {
        val nodeKey = registerNode.services.legalIdentityKey
        val request = RegistrationRequest(nodeRegistration.toWire(nodeKey.private), mapServiceNode.info.address)
        return networkMapService.processRegistrationRequest(request)
    }

    private fun MockNode.registration(
            mapServiceNode: MockNode,
            reg: NodeRegistration,
            privateKey: PrivateKey): ListenableFuture<RegistrationResponse> {
        val req = RegistrationRequest(reg.toWire(privateKey), services.networkService.myAddress)
        return services.networkService.sendRequest(REGISTER_TOPIC, req, mapServiceNode.info.address)
    }

    private fun MockNode.subscribe(mapServiceNode: MockNode, subscribe: Boolean): ListenableFuture<SubscribeResponse> {
        val req = SubscribeRequest(subscribe, services.networkService.myAddress)
        return services.networkService.sendRequest(SUBSCRIPTION_TOPIC, req, mapServiceNode.info.address)
    }

    private fun MockNode.updateAcknowlege(mapServiceNode: MockNode, mapVersion: Int) {
        val req = UpdateAcknowledge(mapVersion, services.networkService.myAddress)
        services.networkService.send(PUSH_ACK_TOPIC, DEFAULT_SESSION_ID, req, mapServiceNode.info.address)
    }

    private fun MockNode.fetchMap(
            mapServiceNode: MockNode,
            subscribe: Boolean,
            ifChangedSinceVersion: Int? = null): Future<Collection<NodeRegistration>?> {
        val net = services.networkService
        val req = FetchMapRequest(subscribe, ifChangedSinceVersion, net.myAddress)
        return net.sendRequest<FetchMapResponse>(FETCH_TOPIC, req, mapServiceNode.info.address).map { it.nodes }
    }
}