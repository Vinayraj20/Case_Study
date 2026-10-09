package com.fde.txn.store;

import com.fde.txn.model.TransactionRecord;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryTransactionStore implements TransactionStore {
    private final Map<String, TransactionRecord> byTxnId = new ConcurrentHashMap<>();
    private final Map<String, String> requestToTxn = new ConcurrentHashMap<>();

    @Override
    public Optional<TransactionRecord> findByTransactionId(String transactionId) {
        return Optional.ofNullable(byTxnId.get(transactionId));
    }

    @Override
    public Optional<String> transactionIdForRequest(String requestId) {
        return Optional.ofNullable(requestToTxn.get(requestId));
    }

    @Override
    public boolean saveIfAbsent(TransactionRecord record, String requestId) {
        if (byTxnId.putIfAbsent(record.request().transactionId(), record) != null) {
            return false;
        }
        requestToTxn.put(requestId, record.request().transactionId());
        return true;
    }

    @Override
    public Collection<TransactionRecord> all() {
        return byTxnId.values();
    }
}
