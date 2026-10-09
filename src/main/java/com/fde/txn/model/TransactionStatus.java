package com.fde.txn.model;

import java.util.EnumSet;
import java.util.Set;

/**
 * Lifecycle of a transaction.
 *
 * <pre>
 * RECEIVED ──► PROCESSING ──► PROCESSED
 *    │             │  ▲
 *    │             ▼  │
 *    │        RETRY_PENDING ──► FAILED (retries exhausted)
 *    ▼             │
 * PENDING_SEQUENCE ┘ (released to PROCESSING once its turn arrives)
 * </pre>
 *
 * DUPLICATE and REJECTED are dispositions of an <em>input record</em> that never becomes (or never
 * changes) a transaction, so they have no transitions; they exist so the summary can count every input.
 */
public enum TransactionStatus {
    RECEIVED,
    PENDING_SEQUENCE,
    PROCESSING,
    RETRY_PENDING,
    PROCESSED,
    FAILED,
    DUPLICATE,
    REJECTED;

    private Set<TransactionStatus> next() {
        return switch (this) {
            case RECEIVED -> EnumSet.of(PROCESSING, PENDING_SEQUENCE);
            case PENDING_SEQUENCE -> EnumSet.of(PROCESSING);
            case PROCESSING -> EnumSet.of(PROCESSED, RETRY_PENDING, FAILED);
            case RETRY_PENDING -> EnumSet.of(PROCESSING, FAILED);
            case PROCESSED, FAILED, DUPLICATE, REJECTED -> EnumSet.noneOf(TransactionStatus.class);
        };
    }

    public boolean canTransitionTo(TransactionStatus target) {
        return next().contains(target);
    }

    public boolean isTerminal() {
        return next().isEmpty();
    }
}
