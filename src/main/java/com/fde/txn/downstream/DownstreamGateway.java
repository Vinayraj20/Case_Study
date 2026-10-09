package com.fde.txn.downstream;

import com.fde.txn.model.TransactionRequest;

/**
 * Stand-in for an external side effect (notification, core-banking call, event publish). The transactionId is the
 * idempotency key a real downstream would honour, so a retried call cannot create a second effect.
 */
public interface DownstreamGateway {
    /** @throws com.fde.txn.ledger.TransientProcessingException on a retryable failure. */
    void publish(TransactionRequest request);
}
