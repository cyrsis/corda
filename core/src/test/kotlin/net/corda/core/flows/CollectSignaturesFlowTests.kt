package net.corda.core.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.contracts.Command
import net.corda.core.contracts.DummyContract
import net.corda.core.contracts.TransactionType
import net.corda.core.contracts.requireThat
import net.corda.core.getOrThrow
import net.corda.core.identity.Party
import net.corda.core.node.PluginServiceHub
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.unwrap
import net.corda.flows.CollectSignaturesFlow
import net.corda.flows.FinalityFlow
import net.corda.flows.SignTransactionFlow
import net.corda.testing.MINI_CORP_KEY
import net.corda.testing.node.MockNetwork
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.util.concurrent.ExecutionException
import kotlin.test.assertFailsWith

class CollectSignaturesFlowTests {
    lateinit var mockNet: MockNetwork
    lateinit var a: MockNetwork.MockNode
    lateinit var b: MockNetwork.MockNode
    lateinit var c: MockNetwork.MockNode
    lateinit var notary: Party

    @Before
    fun setup() {
        mockNet = MockNetwork()
        val nodes = mockNet.createSomeNodes(3)
        a = nodes.partyNodes[0]
        b = nodes.partyNodes[1]
        c = nodes.partyNodes[2]
        notary = nodes.notaryNode.info.notaryIdentity
        mockNet.runNetwork()
        CollectSigsTestCorDapp.registerFlows(a.services)
        CollectSigsTestCorDapp.registerFlows(b.services)
        CollectSigsTestCorDapp.registerFlows(c.services)
    }

    @After
    fun tearDown() {
        mockNet.stopNodes()
    }

    object CollectSigsTestCorDapp {
        // Would normally be called by custom service init in a CorDapp.
        fun registerFlows(pluginHub: PluginServiceHub) {
            pluginHub.registerFlowInitiator(TestFlow.Initiator::class.java) { TestFlow.Responder(it) }
            pluginHub.registerFlowInitiator(TestFlowTwo.Initiator::class.java) { TestFlowTwo.Responder(it) }
        }
    }

    // With this flow, the initiators sends an "offer" to the responder, who then initiates the collect signatures flow.
    // This flow is a more simplifed version of the "TwoPartyTrade" flow and is a useful example of how both the
    // "collectSignaturesFlow" and "SignTransactionFlow" can be used in practise.
    object TestFlow {
        @InitiatingFlow
        class Initiator(val state: DummyContract.MultiOwnerState, val otherParty: Party) : FlowLogic<SignedTransaction>() {
            @Suspendable
            override fun call(): SignedTransaction {
                send(otherParty, state)

                val flow = object : SignTransactionFlow(otherParty) {
                    @Suspendable override fun checkTransaction(stx: SignedTransaction) = requireThat {
                        val tx = stx.tx
                        "There should only be one output state" using (tx.outputs.size == 1)
                        "There should only be one output state" using (tx.inputs.isEmpty())
                        val magicNumberState = tx.outputs.single().data as DummyContract.MultiOwnerState
                        "Must be 1337 or greater" using (magicNumberState.magicNumber >= 1337)
                    }
                }

                val stx = subFlow(flow)
                val ftx = waitForLedgerCommit(stx.id)

                return ftx
            }
        }

        class Responder(val otherParty: Party) : FlowLogic<SignedTransaction>() {
            @Suspendable
            override fun call(): SignedTransaction {
                val state = receive<DummyContract.MultiOwnerState>(otherParty).unwrap { it }
                val notary = serviceHub.networkMapCache.notaryNodes.single().notaryIdentity

                val command = Command(DummyContract.Commands.Create(), state.participants)
                val builder = TransactionType.General.Builder(notary = notary).withItems(state, command)
                val ptx = builder.signWith(serviceHub.legalIdentityKey).toSignedTransaction(false)
                val stx = subFlow(CollectSignaturesFlow(ptx))
                val ftx = subFlow(FinalityFlow(stx)).single()

                return ftx
            }
        }
    }

    // With this flow, the initiator starts the "CollectTransactionFlow". It is then the responders responsibility to
    // override "checkTransaction" and add whatever logic their require to verify the SignedTransaction they are
    // receiving off the wire.
    object TestFlowTwo {
        @InitiatingFlow
        class Initiator(val state: DummyContract.MultiOwnerState, val otherParty: Party) : FlowLogic<SignedTransaction>() {
            @Suspendable
            override fun call(): SignedTransaction {
                val notary = serviceHub.networkMapCache.notaryNodes.single().notaryIdentity
                val command = Command(DummyContract.Commands.Create(), state.participants)
                val builder = TransactionType.General.Builder(notary = notary).withItems(state, command)
                val ptx = builder.signWith(serviceHub.legalIdentityKey).toSignedTransaction(false)
                val stx = subFlow(CollectSignaturesFlow(ptx))
                val ftx = subFlow(FinalityFlow(stx)).single()

                return ftx
            }
        }

        class Responder(val otherParty: Party) : FlowLogic<SignedTransaction>() {
            @Suspendable override fun call(): SignedTransaction {
                val flow = object : SignTransactionFlow(otherParty) {
                    @Suspendable override fun checkTransaction(stx: SignedTransaction) = requireThat {
                        val tx = stx.tx
                        "There should only be one output state" using (tx.outputs.size == 1)
                        "There should only be one output state" using (tx.inputs.isEmpty())
                        val magicNumberState = tx.outputs.single().data as DummyContract.MultiOwnerState
                        "Must be 1337 or greater" using (magicNumberState.magicNumber >= 1337)
                    }
                }

                val stx = subFlow(flow)

                return waitForLedgerCommit(stx.id)
            }
        }
    }


    @Test
    fun `successfully collects two signatures`() {
        val magicNumber = 1337
        val parties = listOf(a.info.legalIdentity, b.info.legalIdentity, c.info.legalIdentity)
        val state = DummyContract.MultiOwnerState(magicNumber, parties.map { it.owningKey })
        val flow = a.services.startFlow(TestFlowTwo.Initiator(state, b.info.legalIdentity))
        mockNet.runNetwork()
        val result = flow.resultFuture.getOrThrow()
        result.verifySignatures()
        println(result.tx)
        println(result.sigs)
    }

    @Test
    fun `no need to collect any signatures`() {
        val onePartyDummyContract = DummyContract.generateInitial(1337, notary, a.info.legalIdentity.ref(1))
        val ptx = onePartyDummyContract.signWith(a.services.legalIdentityKey).toSignedTransaction(false)
        val flow = a.services.startFlow(CollectSignaturesFlow(ptx))
        mockNet.runNetwork()
        val result = flow.resultFuture.getOrThrow()
        result.verifySignatures()
        println(result.tx)
        println(result.sigs)
    }

    @Test
    fun `fails when not signed by initiator`() {
        val onePartyDummyContract = DummyContract.generateInitial(1337, notary, a.info.legalIdentity.ref(1))
        val ptx = onePartyDummyContract.signWith(MINI_CORP_KEY).toSignedTransaction(false)
        val flow = a.services.startFlow(CollectSignaturesFlow(ptx))
        mockNet.runNetwork()
        assertFailsWith<ExecutionException>("The Initiator of CollectSignaturesFlow must have signed the transaction.") {
            flow.resultFuture.get()
        }
    }

    @Test
    fun `passes with multiple initial signatures`() {
        val twoPartyDummyContract = DummyContract.generateInitial(1337, notary,
                a.info.legalIdentity.ref(1),
                b.info.legalIdentity.ref(2),
                b.info.legalIdentity.ref(3))
        val ptx = twoPartyDummyContract.signWith(a.services.legalIdentityKey).signWith(b.services.legalIdentityKey).toSignedTransaction(false)
        val flow = a.services.startFlow(CollectSignaturesFlow(ptx))
        mockNet.runNetwork()
        val result = flow.resultFuture.getOrThrow()
        println(result.tx)
        println(result.sigs)
    }
}

