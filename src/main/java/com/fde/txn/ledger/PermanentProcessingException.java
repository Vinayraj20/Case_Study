package com.fde.txn.ledger;

/** A business-rule failure. Retrying cannot help (e.g. insufficient funds, currency mismatch). */
public class PermanentProcessingException extends RuntimeException {
    public PermanentProcessingException(String message) {
        super(message);
    }
}
