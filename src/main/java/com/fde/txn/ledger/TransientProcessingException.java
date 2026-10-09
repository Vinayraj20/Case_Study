package com.fde.txn.ledger;

/** An infrastructure failure that may succeed if retried (timeout, 503, deadlock...). */
public class TransientProcessingException extends RuntimeException {
    public TransientProcessingException(String message) {
        super(message);
    }
}
