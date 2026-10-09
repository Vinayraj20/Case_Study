package com.fde.txn.validation;

import com.fde.txn.model.RawRecord;
import com.fde.txn.model.TransactionRequest;
import com.fde.txn.model.TransactionType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.regex.Pattern;

public class RequestValidator {
    private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    public ValidationResult validate(RawRecord raw) {
        List<String> errors = new ArrayList<>();
        if (raw.get("_malformed") != null) {
            errors.add("malformed row: " + raw.get("_malformed"));
        }

        String txnId = requireId(raw, "transactionId", errors);
        String reqId = requireId(raw, "requestId", errors);
        String acct = requireId(raw, "accountId", errors);

        OptionalLong seq = OptionalLong.empty();
        String seqRaw = raw.get("sequence");
        if (isBlank(seqRaw)) {
            errors.add("sequence is required");
        } else {
            try {
                long s = Long.parseLong(seqRaw);
                if (s < 1) {
                    errors.add("sequence must be >= 1");
                } else {
                    seq = OptionalLong.of(s);
                }
            } catch (NumberFormatException e) {
                errors.add("sequence must be an integer");
            }
        }

        TransactionType type = null;
        String typeRaw = raw.get("type");
        if (isBlank(typeRaw)) {
            errors.add("type is required");
        } else {
            try {
                type = TransactionType.valueOf(typeRaw.toUpperCase());
            } catch (IllegalArgumentException e) {
                errors.add("type must be one of CREDIT, DEBIT");
            }
        }

        BigDecimal amount = null;
        String amountRaw = raw.get("amount");
        if (isBlank(amountRaw)) {
            errors.add("amount is required");
        } else {
            try {
                amount = new BigDecimal(amountRaw);
                if (amount.signum() <= 0) {
                    errors.add("amount must be positive");
                } else if (amount.scale() > 2) {
                    errors.add("amount must have at most 2 decimal places");
                }
            } catch (NumberFormatException e) {
                errors.add("amount must be numeric");
            }
        }

        String ccy = raw.get("currency");
        if (isBlank(ccy)) {
            errors.add("currency is required");
        } else if (!CURRENCY.matcher(ccy).matches()) {
            errors.add("currency must be a 3-letter upper-case ISO code");
        }

        if (!errors.isEmpty()) {
            return new ValidationResult(null, List.copyOf(errors), seq);
        }
        return new ValidationResult(
                new TransactionRequest(txnId, reqId, seq.getAsLong(), acct, type, amount, ccy), List.of(), seq);
    }

    private static String requireId(RawRecord raw, String field, List<String> errors) {
        String v = raw.get(field);
        if (isBlank(v)) {
            errors.add(field + " is required");
            return null;
        }
        if (!ID.matcher(v).matches()) {
            errors.add(field + " has invalid format");
            return null;
        }
        return v;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
