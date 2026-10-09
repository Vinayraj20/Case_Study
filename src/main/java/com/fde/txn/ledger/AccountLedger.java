package com.fde.txn.ledger;

import com.fde.txn.model.TransactionRequest;
import java.math.BigDecimal;
import java.util.List;

/**
 * The business "side effect": balance + ledger. Business rules live here, away from ingestion and retry logic.
 */
public interface AccountLedger {

    /** Pure pre-check of business rules. @throws PermanentProcessingException if the request can never succeed. */
    void check(TransactionRequest request);

    /**
     * Atomically applies balance change and appends the ledger entry. Idempotent per transactionId: committing
     * the same transaction twice is a no-op, so even a bug in the caller cannot double-apply it.
     */
    LedgerEntry commit(TransactionRequest request);

    BigDecimal balanceOf(String accountId);

    List<LedgerEntry> entries();
}
