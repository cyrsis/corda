package net.corda.node.services.transactions

import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors
import net.corda.core.node.services.TransactionVerifierService
import net.corda.core.transactions.LedgerTransaction
import java.util.concurrent.Executors

class InMemoryTransactionVerifierService(numberOfWorkers: Int) : TransactionVerifierService {
    private val workerPool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numberOfWorkers))

    override fun verify(transaction: LedgerTransaction): ListenableFuture<*> {
        return workerPool.submit {
            transaction.verify()
        }
    }
}
