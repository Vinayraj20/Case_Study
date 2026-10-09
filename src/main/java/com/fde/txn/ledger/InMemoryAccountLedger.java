package com.fde.txn.ledger;

import com.fde.txn.model.TransactionRequest;
import com.fde.txn.model.TransactionType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class InMemoryAccountLedger implements AccountLedger {
    private final Map<String, BigDecimal> balances = new HashMap<>();
    private final Map<String, String> accountCurrency = new HashMap<>();
    private final Map<String, LedgerEntry> entriesByTxn = new LinkedHashMap<>();

    @Override
    public synchronized void check(TransactionRequest r) {
        String ccy = accountCurrency.get(r.accountId());
        if (ccy != null && !ccy.equals(r.currency())) {
            throw new PermanentProcessingException("currency mismatch for account");
        }
        if (r.type() == TransactionType.DEBIT && balanceOf(r.accountId()).compareTo(r.amount()) < 0) {
            throw new PermanentProcessingException("insufficient funds");
        }
    }

    @Override
    public synchronized LedgerEntry commit(TransactionRequest r) {
        LedgerEntry existing = entriesByTxn.get(r.transactionId());
        if (existing != null) {
            return existing; // idempotent: already applied
        }
        check(r); // re-check inside the critical section; nothing is mutated if it throws
        BigDecimal delta = r.type() == TransactionType.CREDIT ? r.amount() : r.amount().negate();
        BigDecimal after = balanceOf(r.accountId()).add(delta);
        // Both mutations happen under one lock => all-or-nothing from any observer's point of view.
        balances.put(r.accountId(), after);
        accountCurrency.putIfAbsent(r.accountId(), r.currency());
        LedgerEntry entry = new LedgerEntry(r.transactionId(), r.accountId(), r.type(), r.amount(), after);
        entriesByTxn.put(r.transactionId(), entry);
        return entry;
    }

    @Override
    public synchronized BigDecimal balanceOf(String accountId) {
        return balances.getOrDefault(accountId, BigDecimal.ZERO);
    }

    @Override
    public synchronized List<LedgerEntry> entries() {
        return new ArrayList<>(entriesByTxn.values());
    }
}
