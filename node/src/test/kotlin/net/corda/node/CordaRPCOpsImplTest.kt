package net.corda.node

import co.paralleluniverse.fibers.Suspendable
import net.corda.contracts.asset.Cash
import net.corda.core.contracts.*
import net.corda.core.crypto.isFulfilledBy
import net.corda.core.crypto.keys
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StateMachineRunId
import net.corda.core.messaging.StateMachineUpdate
import net.corda.core.messaging.startFlow
import net.corda.core.node.services.ServiceInfo
import net.corda.core.node.services.Vault
import net.corda.core.node.services.unconsumedStates
import net.corda.core.serialization.OpaqueBytes
import net.corda.core.transactions.SignedTransaction
import net.corda.flows.CashIssueFlow
import net.corda.flows.CashPaymentFlow
import net.corda.node.internal.CordaRPCOpsImpl
import net.corda.node.services.messaging.CURRENT_RPC_CONTEXT
import net.corda.node.services.messaging.RpcContext
import net.corda.node.services.network.NetworkMapService
import net.corda.node.services.startFlowPermission
import net.corda.node.services.transactions.SimpleNotaryService
import net.corda.node.utilities.transaction
import net.corda.nodeapi.PermissionException
import net.corda.nodeapi.User
import net.corda.testing.expect
import net.corda.testing.expectEvents
import net.corda.testing.node.MockNetwork
import net.corda.testing.node.MockNetwork.MockNode
import net.corda.testing.sequence
import org.apache.commons.io.IOUtils
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.Assert.assertArrayEquals
import org.junit.Before
import org.junit.Test
import rx.Observable
import java.io.ByteArrayOutputStream
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CordaRPCOpsImplTest {

    private companion object {
        val testJar = "net/corda/node/testing/test.jar"
    }

    lateinit var network: MockNetwork
    lateinit var aliceNode: MockNode
    lateinit var notaryNode: MockNode
    lateinit var rpc: CordaRPCOpsImpl
    lateinit var stateMachineUpdates: Observable<StateMachineUpdate>
    lateinit var transactions: Observable<SignedTransaction>
    lateinit var vaultUpdates: Observable<Vault.Update>

    @Before
    fun setup() {
        network = MockNetwork()
        val networkMap = network.createNode(advertisedServices = ServiceInfo(NetworkMapService.type))
        aliceNode = network.createNode(networkMapAddress = networkMap.info.address)
        notaryNode = network.createNode(advertisedServices = ServiceInfo(SimpleNotaryService.type), networkMapAddress = networkMap.info.address)
        rpc = CordaRPCOpsImpl(aliceNode.services, aliceNode.smm, aliceNode.database)
        CURRENT_RPC_CONTEXT.set(RpcContext(User("user", "pwd", permissions = setOf(
                startFlowPermission<CashIssueFlow>(),
                startFlowPermission<CashPaymentFlow>()
        ))))

        aliceNode.database.transaction {
            stateMachineUpdates = rpc.stateMachinesAndUpdates().second
            transactions = rpc.verifiedTransactions().second
            vaultUpdates = rpc.vaultAndUpdates().second
        }
    }

    @Test
    fun `cash issue accepted`() {
        val quantity = 1000L
        val ref = OpaqueBytes(ByteArray(1) { 1 })

        // Check the monitoring service wallet is empty
        aliceNode.database.transaction {
            assertFalse(aliceNode.services.vaultService.unconsumedStates<ContractState>().iterator().hasNext())
        }

        // Tell the monitoring service node to issue some cash
        val recipient = aliceNode.info.legalIdentity
        rpc.startFlow(::CashIssueFlow, Amount(quantity, GBP), ref, recipient, notaryNode.info.notaryIdentity)
        network.runNetwork()

        val expectedState = Cash.State(Amount(quantity,
                Issued(aliceNode.info.legalIdentity.ref(ref), GBP)),
                recipient.owningKey)

        var issueSmId: StateMachineRunId? = null
        stateMachineUpdates.expectEvents {
            sequence(
                    // ISSUE
                    expect { add: StateMachineUpdate.Added ->
                        issueSmId = add.id
                    },
                    expect { remove: StateMachineUpdate.Removed ->
                        require(remove.id == issueSmId)
                    }
            )
        }

        transactions.expectEvents {
            expect { tx ->
                assertEquals(expectedState, tx.tx.outputs.single().data)
            }
        }

        vaultUpdates.expectEvents {
            expect { update ->
                val actual = update.produced.single().state.data
                assertEquals(expectedState, actual)
            }
        }
    }

    @Test
    fun `issue and move`() {
        rpc.startFlow(::CashIssueFlow,
                Amount(100, USD),
                OpaqueBytes(ByteArray(1, { 1 })),
                aliceNode.info.legalIdentity,
                notaryNode.info.notaryIdentity
        )

        network.runNetwork()

        rpc.startFlow(::CashPaymentFlow, Amount(100, USD), aliceNode.info.legalIdentity)

        network.runNetwork()

        var issueSmId: StateMachineRunId? = null
        var moveSmId: StateMachineRunId? = null
        stateMachineUpdates.expectEvents {
            sequence(
                    // ISSUE
                    expect { add: StateMachineUpdate.Added ->
                        issueSmId = add.id
                    },
                    expect { remove: StateMachineUpdate.Removed ->
                        require(remove.id == issueSmId)
                    },
                    // MOVE
                    expect { add: StateMachineUpdate.Added ->
                        moveSmId = add.id
                    },
                    expect { remove: StateMachineUpdate.Removed ->
                        require(remove.id == moveSmId)
                    }
            )
        }

        transactions.expectEvents {
            sequence(
                    // ISSUE
                    expect { stx ->
                        require(stx.tx.inputs.isEmpty())
                        require(stx.tx.outputs.size == 1)
                        val signaturePubKeys = stx.sigs.map { it.by }.toSet()
                        // Only Alice signed
                        val aliceKey = aliceNode.info.legalIdentity.owningKey
                        require(signaturePubKeys.size <= aliceKey.keys.size)
                        require(aliceKey.isFulfilledBy(signaturePubKeys))
                    },
                    // MOVE
                    expect { stx ->
                        require(stx.tx.inputs.size == 1)
                        require(stx.tx.outputs.size == 1)
                        val signaturePubKeys = stx.sigs.map { it.by }.toSet()
                        // Alice and Notary signed
                        require(aliceNode.info.legalIdentity.owningKey.isFulfilledBy(signaturePubKeys))
                        require(notaryNode.info.notaryIdentity.owningKey.isFulfilledBy(signaturePubKeys))
                    }
            )
        }

        vaultUpdates.expectEvents {
            sequence(
                    // ISSUE
                    expect { update ->
                        require(update.consumed.isEmpty()) { update.consumed.size }
                        require(update.produced.size == 1) { update.produced.size }
                    },
                    // MOVE
                    expect { update ->
                        require(update.consumed.size == 1) { update.consumed.size }
                        require(update.produced.size == 1) { update.produced.size }
                    }
            )
        }
    }

    @Test
    fun `cash command by user not permissioned for cash`() {
        CURRENT_RPC_CONTEXT.set(RpcContext(User("user", "pwd", permissions = emptySet())))
        assertThatExceptionOfType(PermissionException::class.java).isThrownBy {
            rpc.startFlow(::CashIssueFlow,
                    Amount(100, USD),
                    OpaqueBytes(ByteArray(1, { 1 })),
                    aliceNode.info.legalIdentity,
                    notaryNode.info.notaryIdentity
            )
        }
    }

    @Test
    fun `can upload an attachment`() {
        val inputJar = Thread.currentThread().contextClassLoader.getResourceAsStream(testJar)
        val secureHash = rpc.uploadAttachment(inputJar)
        assertTrue(rpc.attachmentExists(secureHash))
    }

    @Test
    fun `can download an uploaded attachment`() {
        val inputJar = Thread.currentThread().contextClassLoader.getResourceAsStream(testJar)
        val secureHash = rpc.uploadAttachment(inputJar)
        val bufferFile = ByteArrayOutputStream()
        val bufferRpc = ByteArrayOutputStream()

        IOUtils.copy(Thread.currentThread().contextClassLoader.getResourceAsStream(testJar), bufferFile)
        IOUtils.copy(rpc.openAttachment(secureHash), bufferRpc)

        assertArrayEquals(bufferFile.toByteArray(), bufferRpc.toByteArray())
    }

    @Test
    fun `attempt to start non-RPC flow`() {
        CURRENT_RPC_CONTEXT.set(RpcContext(User("user", "pwd", permissions = setOf(
                startFlowPermission<NonRPCFlow>()
        ))))
        assertThatExceptionOfType(IllegalArgumentException::class.java).isThrownBy {
            rpc.startFlow(::NonRPCFlow)
        }
    }

    class NonRPCFlow : FlowLogic<Unit>() {
        @Suspendable
        override fun call() = Unit
    }
}
