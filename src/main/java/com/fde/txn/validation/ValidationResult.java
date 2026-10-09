package com.fde.txn.validation;

import com.fde.txn.model.TransactionRequest;
import java.util.List;
import java.util.OptionalLong;

/**
 * @param request           present only when valid
 * @param errors            human-readable, field-level (never echoes raw values, to avoid leaking data into logs)
 * @param parsedSequence    the sequence number if it could be parsed even though the record is invalid, so the
 *                          ordering gate can skip that slot instead of waiting for it forever
 */
public record ValidationResult(TransactionRequest request, List<String> errors, OptionalLong parsedSequence) {
    public boolean valid() {
        return errors.isEmpty();
    }
}
