package com.fde.txn.model;

import java.util.Map;

/** One un-validated input row, as produced by a {@code TransactionSource}. Keys are field names. */
public record RawRecord(long lineNumber, Map<String, String> fields) {
    public String get(String name) {
        String v = fields.get(name);
        return v == null ? null : v.trim();
    }
}
