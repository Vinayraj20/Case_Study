package com.fde.txn.ledger;

import com.fde.txn.model.TransactionType;
import java.math.BigDecimal;

public record LedgerEntry(String transactionId, String accountId, TransactionType type, BigDecimal amount,
                          BigDecimal balanceAfter) {}
