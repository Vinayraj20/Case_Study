package com.fde.txn.model;

import java.math.BigDecimal;

/** A validated transaction request. Immutable. */
public record TransactionRequest(
        String transactionId,
        String requestId,
        long sequenceNumber,
        String accountId,
        TransactionType type,
        BigDecimal amount,
        String currency) {

    /** Same business content, ignoring the retry-specific requestId. Used to spot conflicting reuse of a transactionId. */
    public boolean samePayloadAs(TransactionRequest o) {
        return sequenceNumber == o.sequenceNumber
                && accountId.equals(o.accountId)
                && type == o.type
                && amount.compareTo(o.amount) == 0
                && currency.equals(o.currency);
    }
}
