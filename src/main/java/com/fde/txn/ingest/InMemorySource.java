package com.fde.txn.ingest;

import com.fde.txn.model.RawRecord;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InMemorySource implements TransactionSource {
    private final List<RawRecord> records = new ArrayList<>();

    /** Convenience for tests: values in order transactionId, requestId, sequence, accountId, type, amount, currency. */
    public InMemorySource add(String txnId, String reqId, String seq, String acct, String type, String amount, String ccy) {
        Map<String, String> f = new LinkedHashMap<>();
        f.put("transactionId", txnId);
        f.put("requestId", reqId);
        f.put("sequence", seq);
        f.put("accountId", acct);
        f.put("type", type);
        f.put("amount", amount);
        f.put("currency", ccy);
        records.add(new RawRecord(records.size() + 1L, f));
        return this;
    }

    @Override
    public List<RawRecord> read() {
        return List.copyOf(records);
    }
}
