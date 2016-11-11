package net.corda.protocols

import net.corda.core.crypto.Party
import net.corda.core.crypto.SecureHash
import net.corda.core.transactions.SignedTransaction

/**
 * Given a set of tx hashes (IDs), either loads them from local disk or asks the remote peer to provide them.
 *
 * A malicious response in which the data provided by the remote peer does not hash to the requested hash results in
 * [FetchDataProtocol.DownloadedVsRequestedDataMismatch] being thrown. If the remote peer doesn't have an entry, it
 * results in a [FetchDataProtocol.HashNotFound] exception. Note that returned transactions are not inserted into
 * the database, because it's up to the caller to actually verify the transactions are valid.
 */
class FetchTransactionsProtocol(requests: Set<SecureHash>, otherSide: Party) :
        FetchDataProtocol<SignedTransaction, SignedTransaction>(requests, otherSide) {

    override fun load(txid: SecureHash): SignedTransaction? = serviceHub.storageService.validatedTransactions.getTransaction(txid)
}