package com.fde.txn.processing;

import com.fde.txn.ledger.AccountLedger;
import com.fde.txn.model.TransactionRecord;
import com.fde.txn.model.TransactionStatus;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Point-in-time operational report. Every input record is accounted for in exactly one status bucket. */
public record ProcessingSummary(
        int totalInputRecords,
        Map<TransactionStatus, Integer> countsByStatus,
        int totalRetries,
        List<Long> missingSequences,
        long nextExpectedSequence,
        List<String> failedTransactions,
        List<String> pendingTransactions,
        List<Rejection> rejections,
        Map<String, BigDecimal> balances) {

    static ProcessingSummary from(Collection<TransactionRecord> records, List<DuplicateEvent> duplicates,
                                  List<Rejection> rejections, SequenceGate gate, AccountLedger ledger) {
        Map<TransactionStatus, Integer> counts = new EnumMap<>(TransactionStatus.class);
        for (TransactionStatus s : TransactionStatus.values()) {
            counts.put(s, 0);
        }
        int retries = 0;
        List<String> failed = new java.util.ArrayList<>();
        List<String> pending = new java.util.ArrayList<>();
        for (TransactionRecord r : records) {
            counts.merge(r.status(), 1, Integer::sum);
            retries += Math.max(0, r.attempts() - 1);
            String id = r.request().transactionId();
            if (r.status() == TransactionStatus.FAILED) {
                failed.add(id + " (" + r.failureReason() + ")");
            } else if (r.status() == TransactionStatus.PENDING_SEQUENCE) {
                pending.add(id + " (seq " + r.request().sequenceNumber() + ")");
            }
        }
        counts.put(TransactionStatus.DUPLICATE, duplicates.size());
        counts.put(TransactionStatus.REJECTED, rejections.size());
        int total = counts.values().stream().mapToInt(Integer::intValue).sum();

        Map<String, BigDecimal> balances = new TreeMap<>();
        ledger.entries().forEach(e -> balances.put(e.accountId(), ledger.balanceOf(e.accountId())));

        failed.sort(Comparator.naturalOrder());
        pending.sort(Comparator.naturalOrder());
        return new ProcessingSummary(total, counts, retries, gate.missingSequences(), gate.nextExpected(),
                failed, pending, List.copyOf(rejections), balances);
    }

    public int count(TransactionStatus s) {
        return countsByStatus.getOrDefault(s, 0);
    }

    public String render() {
        StringBuilder sb = new StringBuilder();
        sb.append("==================== PROCESSING SUMMARY ====================\n");
        sb.append(String.format("Input records            : %d%n", totalInputRecords));
        for (TransactionStatus s : TransactionStatus.values()) {
            sb.append(String.format("  %-22s : %d%n", s, count(s)));
        }
        sb.append(String.format("Retries performed        : %d%n", totalRetries));
        sb.append(String.format("Next expected sequence   : %d%n", nextExpectedSequence));
        sb.append(String.format("Missing sequences        : %s%n", missingSequences.isEmpty() ? "none" : missingSequences));
        if (!failedTransactions.isEmpty()) {
            sb.append("Failed (needs recovery)  :\n");
            failedTransactions.forEach(f -> sb.append("  - ").append(f).append('\n'));
        }
        if (!pendingTransactions.isEmpty()) {
            sb.append("Still pending (blocked)  :\n");
            pendingTransactions.forEach(p -> sb.append("  - ").append(p).append('\n'));
        }
        if (!rejections.isEmpty()) {
            sb.append("Rejected (dead letter)   :\n");
            rejections.forEach(r -> sb.append(String.format("  - line %d [%s]: %s%n",
                    r.lineNumber(), r.transactionId(), String.join("; ", r.reasons()))));
        }
        sb.append("Balances                 : ").append(balances).append('\n');
        sb.append("============================================================");
        return sb.toString();
    }
}
