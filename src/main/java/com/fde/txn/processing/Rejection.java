package com.fde.txn.processing;

import java.util.List;

/** Dead-letter entry: an input record that was refused. Stores reasons, not raw values. */
public record Rejection(long lineNumber, String requestId, String transactionId, List<String> reasons) {}
