package com.fde.txn.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Mutable processing state of one unique business transaction, with a full audit trail of transitions. */
public class TransactionRecord {

    public record StatusChange(Instant at, TransactionStatus status, String note) {}

    private final TransactionRequest request;
    private TransactionStatus status = TransactionStatus.RECEIVED;
    private int attempts;
    private String failureReason;
    private final List<StatusChange> history = new ArrayList<>();

    public TransactionRecord(TransactionRequest request) {
        this.request = request;
        history.add(new StatusChange(Instant.now(), status, "received"));
    }

    public void transitionTo(TransactionStatus target, String note) {
        if (!status.canTransitionTo(target)) {
            throw new IllegalStateException(
                    "Illegal transition " + status + " -> " + target + " for " + request.transactionId());
        }
        status = target;
        if (target == TransactionStatus.FAILED) {
            failureReason = note;
        }
        history.add(new StatusChange(Instant.now(), target, note));
    }

    public void recordAttempt() {
        attempts++;
    }

    public TransactionRequest request() { return request; }
    public TransactionStatus status() { return status; }
    public int attempts() { return attempts; }
    public String failureReason() { return failureReason; }
    public List<StatusChange> history() { return Collections.unmodifiableList(history); }
}
