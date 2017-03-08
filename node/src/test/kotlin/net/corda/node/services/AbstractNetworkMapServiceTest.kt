package net.corda.node.services

import com.google.common.util.concurrent.ListenableFuture
import net.corda.core.getOrThrow
import net.corda.core.map
import net.corda.core.messaging.send
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
import net.corda.testing.node.MockNetwork
import org.assertj.core.api.Assertions.assertThat
import java.security.PrivateKey
import java.time.Instant
import java.util.concurrent.Future
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Abstracted out test logic to be re-used by [PersistentNetworkMapServiceTest].
 */
abstract class AbstractNetworkMapServiceTest {

    protected fun success(mapServiceNode: MockNetwork.MockNode,
                          registerNode: MockNetwork.MockNode,
                          service: () -> AbstractNetworkMapService,
                          swizzle: () -> Unit) {
        // For persistent service, switch out the implementation for a newly instantiated one so we can check the state is preserved.
        swizzle()

        // Confirm the service contains no nodes as own node only registered if network is run.
        assertEquals(0, service().nodes.count())
        val identityRequest = QueryIdentityRequest(registerNode.info.legalIdentity, mapServiceNode.info.address, Long.MIN_VALUE)
        assertNull(service().processQueryRequest(identityRequest).node)

        // Register the new node
        val instant = Instant.now()
        val expires = instant + NetworkMapService.DEFAULT_EXPIRATION_PERIOD
        val nodeKey = registerNode.services.legalIdentityKey
        val addChange = NodeRegistration(registerNode.info, instant.toEpochMilli(), AddOrRemove.ADD, expires)
        val addWireChange = addChange.toWire(nodeKey.private)
        val registerRequest = RegistrationRequest(addWireChange, mapServiceNode.info.address, Long.MIN_VALUE)
        service().processRegistrationChangeRequest(registerRequest)
        swizzle()

        assertEquals(1, service().nodes.count())
        assertEquals(registerNode.info, service().processQueryRequest(identityRequest).node)

        // Re-registering should be a no-op
        assertThat(service().processRegistrationChangeRequest(registerRequest).error).isNull()
        swizzle()

        assertEquals(1, service().nodes.count())

        // Confirm that de-registering the node succeeds and drops it from the node lists
        val removeChange = NodeRegistration(registerNode.info, instant.toEpochMilli() + 1, AddOrRemove.REMOVE, expires)
        val removeWireChange = removeChange.toWire(nodeKey.private)
        val deRegisterRequest = RegistrationRequest(removeWireChange, mapServiceNode.info.address, Long.MIN_VALUE)
        assertThat(service().processRegistrationChangeRequest(deRegisterRequest).error).isNull()
        swizzle()

        assertNull(service().processQueryRequest(identityRequest).node)
        swizzle()

        // Trying to de-register a node that doesn't exist should fail
        assertThat(service().processRegistrationChangeRequest(deRegisterRequest).error).isNotNull()
    }

    protected fun `success with network`(network: MockNetwork,
                                         mapServiceNode: MockNetwork.MockNode,
                                         registerNode: MockNetwork.MockNode,
                                         swizzle: () -> Unit) {
        // For persistent service, switch out the implementation for a newly instantiated one so we can check the state is preserved.
        swizzle()

        // Confirm all nodes have registered themselves
        network.runNetwork()
        var fetchResult = registerNode.fetchMap(mapServiceNode, false)
        network.runNetwork()
        assertEquals(2, fetchResult.getOrThrow()?.count())

        // Forcibly deregister the second node
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
        assertEquals(mapServiceNode.info, fetchResult.getOrThrow()?.filter { it.type == AddOrRemove.ADD }?.map { it.node }?.single())
    }

    protected fun `subscribe with network`(network: MockNetwork,
                                           mapServiceNode: MockNetwork.MockNode,
                                           registerNode: MockNetwork.MockNode,
                                           service: () -> AbstractNetworkMapService,
                                           swizzle: () -> Unit) {
        // For persistent service, switch out the implementation for a newly instantiated one so we can check the state is preserved.
        swizzle()

        // Test subscribing to updates
        network.runNetwork()
        val subscribeResult = registerNode.subscribe(mapServiceNode, true)
        network.runNetwork()
        subscribeResult.getOrThrow()

        swizzle()

        val startingMapVersion = service().mapVersion

        // Check the unacknowledged count is zero
        assertEquals(0, service().getUnacknowledgedCount(registerNode.info.address, startingMapVersion))

        // Fire off an update
        val nodeKey = registerNode.services.legalIdentityKey
        var seq = 0L
        val expires = Instant.now() + NetworkMapService.DEFAULT_EXPIRATION_PERIOD
        var reg = NodeRegistration(registerNode.info, seq++, AddOrRemove.ADD, expires)
        var wireReg = reg.toWire(nodeKey.private)
        service().notifySubscribers(wireReg, startingMapVersion + 1)

        swizzle()

        // Check the unacknowledged count is one
        assertEquals(1, service().getUnacknowledgedCount(registerNode.info.address, startingMapVersion + 1))

        // Send in an acknowledgment and verify the count goes down
        registerNode.updateAcknowlege(mapServiceNode, startingMapVersion + 1)
        network.runNetwork()

        swizzle()

        assertEquals(0, service().getUnacknowledgedCount(registerNode.info.address, startingMapVersion + 1))

        // Intentionally fill the pending acknowledgements to verify it doesn't drop subscribers before the limit
        // is hit. On the last iteration overflow the pending list, and check the node is unsubscribed
        for (i in 0..service().maxUnacknowledgedUpdates) {
            reg = NodeRegistration(registerNode.info, seq++, AddOrRemove.ADD, expires)
            wireReg = reg.toWire(nodeKey.private)
            service().notifySubscribers(wireReg, i + startingMapVersion + 2)

            swizzle()

            if (i < service().maxUnacknowledgedUpdates) {
                assertEquals(i + 1, service().getUnacknowledgedCount(registerNode.info.address, i + startingMapVersion + 2))
            } else {
                assertNull(service().getUnacknowledgedCount(registerNode.info.address, i + startingMapVersion + 2))
            }
        }
    }

    private fun MockNetwork.MockNode.registration(
            mapServiceNode: MockNetwork.MockNode,
            reg: NodeRegistration,
            privateKey: PrivateKey): ListenableFuture<RegistrationResponse> {
        val req = RegistrationRequest(reg.toWire(privateKey), services.networkService.myAddress)
        return services.networkService.sendRequest(REGISTER_TOPIC, req, mapServiceNode.info.address)
    }

    private fun MockNetwork.MockNode.subscribe(mapServiceNode: MockNetwork.MockNode, subscribe: Boolean): ListenableFuture<SubscribeResponse> {
        val req = SubscribeRequest(subscribe, services.networkService.myAddress)
        return services.networkService.sendRequest(SUBSCRIPTION_TOPIC, req, mapServiceNode.info.address)
    }

    private fun MockNetwork.MockNode.updateAcknowlege(mapServiceNode: MockNetwork.MockNode, mapVersion: Int) {
        val req = UpdateAcknowledge(mapVersion, services.networkService.myAddress)
        services.networkService.send(PUSH_ACK_TOPIC, DEFAULT_SESSION_ID, req, mapServiceNode.info.address)
    }

    private fun MockNetwork.MockNode.fetchMap(
            mapServiceNode: MockNetwork.MockNode,
            subscribe: Boolean,
            ifChangedSinceVersion: Int? = null): Future<Collection<NodeRegistration>?> {
        val net = services.networkService
        val req = FetchMapRequest(subscribe, ifChangedSinceVersion, net.myAddress)
        return net.sendRequest<NetworkMapService.FetchMapResponse>(FETCH_TOPIC, req, mapServiceNode.info.address).map { it.nodes }
    }
}