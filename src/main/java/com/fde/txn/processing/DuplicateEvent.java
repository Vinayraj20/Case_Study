package com.fde.txn.processing;

public record DuplicateEvent(long lineNumber, String requestId, String transactionId) {}
