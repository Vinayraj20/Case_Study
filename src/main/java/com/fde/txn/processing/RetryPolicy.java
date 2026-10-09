package com.fde.txn.processing;

/** @param maxAttempts total attempts including the first (3 => 1 try + 2 retries). */
public record RetryPolicy(int maxAttempts, long backoffMillis) {
    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
    }

    /** Exponential backoff: base, 2x base, 4x base ... capped at 30s. */
    public long delayBeforeRetry(int failedAttempt) {
        long d = backoffMillis * (1L << Math.min(failedAttempt - 1, 10));
        return Math.min(d, 30_000);
    }
}
