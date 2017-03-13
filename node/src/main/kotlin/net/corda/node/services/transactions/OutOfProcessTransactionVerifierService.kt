package net.corda.node.services.transactions

import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.SettableFuture
import net.corda.core.node.services.TransactionVerifierService
import net.corda.core.random63BitValue
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.transactions.LedgerTransaction
import net.corda.node.VerifierApi
import org.apache.activemq.artemis.api.core.client.ClientConsumer
import java.util.concurrent.ConcurrentHashMap

abstract class OutOfProcessTransactionVerifierService : SingletonSerializeAsToken(), TransactionVerifierService {
    private val verificationResponseFutures = ConcurrentHashMap<Long, SettableFuture<Unit>>()

    class VerificationResultForUnknownTransaction(nonce: Long) : Exception("Verification result arrived for unknown transaction nonce $nonce")

    fun start(responseConsumer: ClientConsumer) {
        responseConsumer.setMessageHandler { message ->
            val response = VerifierApi.VerificationResponse.fromClientMessage(message)
            val resultFuture = verificationResponseFutures.remove(response.verificationId) ?:
                    throw VerificationResultForUnknownTransaction(response.verificationId)
            val exception = response.exception
            if (exception == null) {
                resultFuture.set(Unit)
            } else {
                resultFuture.setException(exception)
            }
        }
    }

    abstract fun sendRequest(nonce: Long, transaction: LedgerTransaction)

    override fun verify(transaction: LedgerTransaction): ListenableFuture<*> {
        val future = SettableFuture.create<Unit>()
        val nonce = random63BitValue()
        verificationResponseFutures[nonce] = future
        sendRequest(nonce, transaction)

        return future
    }
}