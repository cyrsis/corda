package net.corda.verifier

import com.google.common.util.concurrent.Futures
import net.corda.client.mock.generateOrFail
import net.corda.core.contracts.DOLLARS
import net.corda.core.messaging.startFlow
import net.corda.core.node.services.ServiceInfo
import net.corda.core.serialization.OpaqueBytes
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.transactions.WireTransaction
import net.corda.flows.CashIssueFlow
import net.corda.node.services.config.VerifierType
import net.corda.node.services.transactions.ValidatingNotaryService
import org.junit.Test
import java.util.*
import kotlin.test.assertEquals

class VerifierTests {
    private fun generateTransactions(number: Int): List<LedgerTransaction> {
        var currentLedger = GeneratedLedger.empty
        val transactions = ArrayList<WireTransaction>()
        val random = SplittableRandom()
        for (i in 0..number - 1) {
            val (tx, ledger) = currentLedger.transactionGenerator.generateOrFail(random)
            transactions.add(tx)
            currentLedger = ledger
        }
        return transactions.map { currentLedger.resolveWireTransaction(it) }
    }

    @Test
    fun singleVerifierWorksWithRequestor() {
        verifierDriver(automaticallyStartNetworkMap = false) {
            val aliceFuture = startVerificationRequestor("Alice")
            val transactions = generateTransactions(100)
            val alice = aliceFuture.get()
            startVerifier(alice)
            alice.waitUntilNumberOfVerifiers(1)
            val results = Futures.allAsList(transactions.map { alice.verifyTransaction(it) }).get()
            results.forEach {
                if (it != null) {
                    throw it
                }
            }
        }
    }

    @Test
    fun multipleVerifiersWorkWithRequestor() {
        verifierDriver(automaticallyStartNetworkMap = false) {
            val aliceFuture = startVerificationRequestor("Alice")
            val transactions = generateTransactions(100)
            val alice = aliceFuture.get()
            for (i in 1..4) {
                startVerifier(alice)
            }
            alice.waitUntilNumberOfVerifiers(10)
            val results = Futures.allAsList(transactions.map { alice.verifyTransaction(it) }).get()
            results.forEach {
                if (it != null) {
                    throw it
                }
            }
        }
    }

    @Test
    fun singleVerifierWorksWithNode() {
        verifierDriver {
            val aliceFuture = startNode("Alice", verifierType = VerifierType.OutOfProcess)
            val notaryFuture = startNode("Notary", advertisedServices = setOf(ServiceInfo(ValidatingNotaryService.type)))
            val alice = aliceFuture.get()
            startVerifier(alice)
            alice.waitUntilNumberOfVerifiers(1)
            for (i in 1..10) {
                val result = alice.rpc.startFlow(::CashIssueFlow, 10.DOLLARS, OpaqueBytes.of(0), alice.nodeInfo.legalIdentity, notaryFuture.get().nodeInfo.notaryIdentity)
                result.returnValue.get()
            }
        }
    }
}