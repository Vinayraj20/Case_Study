package com.fde.txn.observability;

public final class Masking {
    private Masking() {}

    /** Keeps only the last 3 characters of an identifier: ACC-201 -> ****201. */
    public static String mask(String value) {
        if (value == null) {
            return "null";
        }
        int keep = Math.min(3, value.length());
        return "****" + value.substring(value.length() - keep);
    }
}
