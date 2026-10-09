package com.fde.txn.store;

import com.fde.txn.model.TransactionRecord;
import java.util.Collection;
import java.util.Optional;

/**
 * Transaction state + idempotency store. In production: a table with UNIQUE(transaction_id) and UNIQUE(request_id);
 * {@link #saveIfAbsent} maps to INSERT ... ON CONFLICT DO NOTHING.
 */
public interface TransactionStore {
    Optional<TransactionRecord> findByTransactionId(String transactionId);

    Optional<String> transactionIdForRequest(String requestId);

    /** @return true if inserted, false if the transactionId already existed (atomic). */
    boolean saveIfAbsent(TransactionRecord record, String requestId);

    Collection<TransactionRecord> all();
}
