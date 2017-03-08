package net.corda.node

import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.transactions.LedgerTransaction
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.client.ClientMessage
import org.apache.activemq.artemis.reader.MessageUtil

class VerifierApi {
    companion object {
        val VERIFIER_USERNAME = "SystemUsers/Verifier"
        val VERIFICATION_REQUESTS_QUEUE_NAME = "verifier.requests"
        val VERIFICATION_RESPONSES_QUEUE_NAME_PREFIX = "verifier.responses"
        private val VERIFICATION_ID_FIELD_NAME = "id"
        private val TRANSACTION_FIELD_NAME = "transaction"
        private val RESULT_EXCEPTION_FIELD_NAME = "result-exception"
    }

    data class VerificationRequest(
            val verificationId: Long,
            val transaction: LedgerTransaction,
            val responseAddress: SimpleString
    ) {
        companion object {
            fun fromClientMessage(message: ClientMessage): VerificationRequest {
                return VerificationRequest(
                        message.getLongProperty(VERIFICATION_ID_FIELD_NAME),
                        message.getBytesProperty(TRANSACTION_FIELD_NAME).deserialize(),
                        MessageUtil.getJMSReplyTo(message)
                )
            }
        }

        fun writeToClientMessage(message: ClientMessage) {
            message.putLongProperty(VERIFICATION_ID_FIELD_NAME, verificationId)
            message.putBytesProperty(TRANSACTION_FIELD_NAME, transaction.serialize().bytes)
            MessageUtil.setJMSReplyTo(message, responseAddress)
        }
    }

    data class VerificationResponse(
            val verificationId: Long,
            val exception: Throwable?
    ) {
        companion object {
            fun fromClientMessage(message: ClientMessage): VerificationResponse {
                return VerificationResponse(
                        message.getLongProperty(VERIFICATION_ID_FIELD_NAME),
                        message.getBytesProperty(RESULT_EXCEPTION_FIELD_NAME)?.deserialize()
                )
            }
        }

        fun writeToClientMessage(message: ClientMessage) {
            message.putLongProperty(VERIFICATION_ID_FIELD_NAME, verificationId)
            if (exception != null) {
                message.putBytesProperty(TRANSACTION_FIELD_NAME, exception.serialize().bytes)
            }
        }
    }

}
