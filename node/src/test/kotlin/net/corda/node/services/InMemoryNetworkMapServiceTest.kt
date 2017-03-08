package net.corda.node.services

import net.corda.node.services.network.InMemoryNetworkMapService
import net.corda.node.utilities.databaseTransaction
import net.corda.testing.node.MockNetwork
import org.junit.Before
import org.junit.Test
import kotlin.test.assertNotNull

class InMemoryNetworkMapServiceTest : AbstractNetworkMapServiceTest() {
    lateinit var network: MockNetwork

    @Before
    fun setup() {
        network = MockNetwork()
    }

    /**
     * Perform basic tests of registering, de-registering and fetching the full network map.
     */
    @Test
    fun success() {
        val (mapServiceNode, registerNode) = network.createTwoNodes()
        val service = mapServiceNode.inNodeNetworkMapService!! as InMemoryNetworkMapService
        databaseTransaction(mapServiceNode.database) {
            success(mapServiceNode, registerNode, { service }, { })
        }
    }

    @Test
    fun `success with network`() {
        val (mapServiceNode, registerNode) = network.createTwoNodes()

        // Confirm there's a network map service on node 0
        assertNotNull(mapServiceNode.inNodeNetworkMapService)
        `success with network`(network, mapServiceNode, registerNode, { })
    }

    @Test
    fun `subscribe with network`() {
        val (mapServiceNode, registerNode) = network.createTwoNodes()
        val service = (mapServiceNode.inNodeNetworkMapService as InMemoryNetworkMapService)
        `subscribe with network`(network, mapServiceNode, registerNode, { service }, { })
    }
}
