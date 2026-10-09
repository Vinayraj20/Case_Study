package com.fde.txn;

import static org.junit.jupiter.api.Assertions.*;

import com.fde.txn.downstream.SimulatedDownstreamGateway;
import com.fde.txn.ingest.InMemorySource;
import com.fde.txn.ledger.InMemoryAccountLedger;
import com.fde.txn.ledger.LedgerEntry;
import com.fde.txn.model.RawRecord;
import com.fde.txn.model.TransactionStatus;
import com.fde.txn.observability.EventLog;
import com.fde.txn.processing.ProcessingSummary;
import com.fde.txn.processing.RetryPolicy;
import com.fde.txn.processing.TransactionProcessor;
import com.fde.txn.store.InMemoryTransactionStore;
import com.fde.txn.validation.RequestValidator;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TransactionProcessorTest {

    private SimulatedDownstreamGateway downstream;
    private InMemoryAccountLedger ledger;
    private EventLog log;
    private TransactionProcessor processor;

    @BeforeEach
    void setUp() {
        downstream = new SimulatedDownstreamGateway();
        ledger = new InMemoryAccountLedger();
        log = EventLog.silent();
        processor = new TransactionProcessor(new RequestValidator(), new InMemoryTransactionStore(), ledger,
                downstream, new RetryPolicy(3, 0), log, 1, millis -> {});
    }

    private void run(InMemorySource source) {
        for (RawRecord r : source.read()) {
            processor.ingest(r);
        }
    }

    private TransactionStatus status(String txnId) {
        return processor.find(txnId).orElseThrow().status();
    }

    private static BigDecimal bd(String s) { return new BigDecimal(s); }

    // ---------- the suggested test data from the problem statement ----------

    @Test
    void suggestedDataSet_endToEnd() {
        downstream.failFirst("TXN-2005", 1);
        run(new InMemorySource()
                .add("TXN-2002", "REQ-2", "2", "ACC-201", "DEBIT", "40", "USD")
                .add("TXN-2001", "REQ-1", "1", "ACC-201", "CREDIT", "100", "USD")
                .add("TXN-2002", "REQ-2-DUP", "2", "ACC-201", "DEBIT", "40", "USD")
                .add("TXN-2003", "REQ-3", "3", "ACC-201", "CREDIT", "25", "USD")
                .add("TXN-2004", "REQ-4", "4", "ACC-202", "DEBIT", "-10", "USD")
                .add("TXN-2005", "REQ-5", "5", "ACC-203", "CREDIT", "500", "USD"));

        ProcessingSummary s = processor.summary();
        assertEquals(6, s.totalInputRecords());
        assertEquals(4, s.count(TransactionStatus.PROCESSED));
        assertEquals(1, s.count(TransactionStatus.DUPLICATE));
        assertEquals(1, s.count(TransactionStatus.REJECTED));
        assertEquals(0, s.count(TransactionStatus.FAILED));
        assertEquals(1, s.totalRetries());
        assertEquals(0, bd("85").compareTo(ledger.balanceOf("ACC-201")));
        assertEquals(0, bd("500").compareTo(ledger.balanceOf("ACC-203")));
        assertEquals(2, processor.find("TXN-2005").orElseThrow().attempts());
    }

    // ---------- idempotency / duplicates ----------

    @Test
    void duplicateAfterProcessing_hasNoSecondEffect() {
        run(new InMemorySource()
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "100", "USD")
                .add("TXN-1", "REQ-1-RETRY", "1", "ACC-1", "CREDIT", "100", "USD")
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "100", "USD")); // exact replay

        assertEquals(0, bd("100").compareTo(ledger.balanceOf("ACC-1")));
        assertEquals(1, ledger.entries().size());
        assertEquals(1, downstream.distinctEffects());
        assertEquals(1, downstream.totalCalls());
        assertEquals(2, processor.summary().count(TransactionStatus.DUPLICATE));
    }

    @Test
    void duplicateWhileOriginalIsStillPending_isDetected() {
        run(new InMemorySource()
                .add("TXN-2", "REQ-2", "2", "ACC-1", "CREDIT", "10", "USD")
                .add("TXN-2", "REQ-2-DUP", "2", "ACC-1", "CREDIT", "10", "USD")  // dup of a buffered txn
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "5", "USD"));

        assertEquals(1, processor.summary().count(TransactionStatus.DUPLICATE));
        assertEquals(2, ledger.entries().size());
        assertEquals(0, bd("15").compareTo(ledger.balanceOf("ACC-1")));
    }

    @Test
    void sameTransactionIdWithDifferentPayload_isRejectedNotAppliedNorTreatedAsDuplicate() {
        run(new InMemorySource()
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "100", "USD")
                .add("TXN-1", "REQ-X", "1", "ACC-1", "CREDIT", "999", "USD"));

        assertEquals(1, processor.summary().count(TransactionStatus.REJECTED));
        assertEquals(0, processor.summary().count(TransactionStatus.DUPLICATE));
        assertEquals(0, bd("100").compareTo(ledger.balanceOf("ACC-1")));
    }

    @Test
    void requestIdReusedForDifferentTransaction_isRejected() {
        run(new InMemorySource()
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "100", "USD")
                .add("TXN-2", "REQ-1", "2", "ACC-1", "CREDIT", "100", "USD"));

        assertEquals(1, processor.summary().count(TransactionStatus.REJECTED));
        assertTrue(processor.find("TXN-2").isEmpty());
    }

    // ---------- ordering ----------

    @Test
    void outOfOrder_debitWaitsForCredit_andIsAppliedInSequenceOrder() {
        InMemorySource src = new InMemorySource()
                .add("TXN-2", "REQ-2", "2", "ACC-1", "DEBIT", "40", "USD")
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "100", "USD");

        processor.ingest(src.read().get(0));
        assertEquals(TransactionStatus.PENDING_SEQUENCE, status("TXN-2")); // buffered, NOT failed for lack of funds
        assertTrue(ledger.entries().isEmpty());

        processor.ingest(src.read().get(1));
        assertEquals(TransactionStatus.PROCESSED, status("TXN-1"));
        assertEquals(TransactionStatus.PROCESSED, status("TXN-2"));

        List<String> order = ledger.entries().stream().map(LedgerEntry::transactionId).toList();
        assertEquals(List.of("TXN-1", "TXN-2"), order);
        assertEquals(0, bd("60").compareTo(ledger.balanceOf("ACC-1")));
    }

    @Test
    void fullyReversedArrival_stillProcessesInOrder() {
        run(new InMemorySource()
                .add("TXN-3", "REQ-3", "3", "ACC-1", "DEBIT", "10", "USD")
                .add("TXN-2", "REQ-2", "2", "ACC-1", "DEBIT", "20", "USD")
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "100", "USD"));

        assertEquals(List.of("TXN-1", "TXN-2", "TXN-3"),
                ledger.entries().stream().map(LedgerEntry::transactionId).toList());
        assertEquals(0, bd("70").compareTo(ledger.balanceOf("ACC-1")));
    }

    @Test
    void missingSequence_isReported_andBlockedWorkStaysPending() {
        run(new InMemorySource()
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "10", "USD")
                .add("TXN-4", "REQ-4", "4", "ACC-1", "CREDIT", "10", "USD"));

        ProcessingSummary s = processor.summary();
        assertEquals(List.of(2L, 3L), s.missingSequences());
        assertEquals(1, s.count(TransactionStatus.PENDING_SEQUENCE));
        assertEquals(1, ledger.entries().size());
    }

    @Test
    void lateArrivalOfAlreadyConsumedSequence_isRejected() {
        run(new InMemorySource()
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "10", "USD")
                .add("TXN-OTHER", "REQ-O", "1", "ACC-1", "CREDIT", "10", "USD"));

        assertEquals(1, processor.summary().count(TransactionStatus.REJECTED));
        assertEquals(1, ledger.entries().size());
    }

    // ---------- validation ----------

    @Test
    void invalidRecord_isIsolated_andDoesNotBlockLaterSequences() {
        run(new InMemorySource()
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "10", "USD")
                .add("TXN-2", "REQ-2", "2", "ACC-1", "DEBIT", "-10", "USD")     // invalid, but slot 2 must be freed
                .add("TXN-3", "REQ-3", "3", "ACC-1", "CREDIT", "10", "USD"));

        ProcessingSummary s = processor.summary();
        assertEquals(2, s.count(TransactionStatus.PROCESSED));
        assertEquals(1, s.count(TransactionStatus.REJECTED));
        assertEquals(0, s.count(TransactionStatus.PENDING_SEQUENCE));
    }

    @Test
    void invalidRecordArrivingEarly_freesItsSlotWhenItsTurnComes() {
        run(new InMemorySource()
                .add("TXN-2", "REQ-2", "2", "ACC-1", "DEBIT", "oops", "USD")    // resolved ahead of its turn
                .add("TXN-3", "REQ-3", "3", "ACC-1", "CREDIT", "10", "USD")
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "10", "USD"));

        assertEquals(TransactionStatus.PROCESSED, status("TXN-3"));
        assertEquals(1, processor.summary().count(TransactionStatus.REJECTED));
    }

    @Test
    void malformedAndMissingFields_areRejectedWithoutThrowing() {
        InMemorySource src = new InMemorySource()
                .add(null, "REQ-1", "1", "ACC-1", "CREDIT", "10", "USD")
                .add("TXN-2", "REQ-2", "abc", "ACC-1", "CREDIT", "10", "USD")
                .add("TXN-3", "REQ-3", "3", "ACC-1", "TRANSFER", "10", "USD")
                .add("TXN-4", "REQ-4", "4", "ACC-1", "CREDIT", "10", "usd")
                .add("TXN-5", "REQ-5", "5", "ACC-1", "CREDIT", "10.123", "USD");
        assertDoesNotThrow(() -> run(src));
        assertEquals(5, processor.summary().count(TransactionStatus.REJECTED));
        assertTrue(ledger.entries().isEmpty());
    }

    @Test
    void rejectionsDoNotLeakRawValues() {
        run(new InMemorySource().add("TXN-1", "REQ-1", "1", "ACC-SECRET-9", "CREDIT", "not-a-number", "USD"));
        String reasons = String.join(";", processor.rejections().get(0).reasons());
        assertFalse(reasons.contains("not-a-number"));
    }

    // ---------- resilience ----------

    @Test
    void transientFailure_isRetried_andProducesExactlyOneEffect() {
        downstream.failFirst("TXN-1", 2); // fails twice, succeeds on 3rd attempt (max 3)
        run(new InMemorySource().add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "100", "USD"));

        assertEquals(TransactionStatus.PROCESSED, status("TXN-1"));
        assertEquals(3, processor.find("TXN-1").orElseThrow().attempts());
        assertEquals(1, ledger.entries().size());
        assertEquals(0, bd("100").compareTo(ledger.balanceOf("ACC-1")));
        assertEquals(1, downstream.distinctEffects());
    }

    @Test
    void lifecycleHistory_recordsRetryPath() {
        downstream.failFirst("TXN-1", 1);
        run(new InMemorySource().add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "100", "USD"));

        List<TransactionStatus> path = processor.find("TXN-1").orElseThrow().history().stream()
                .map(h -> h.status()).toList();
        assertEquals(List.of(TransactionStatus.RECEIVED, TransactionStatus.PROCESSING,
                TransactionStatus.RETRY_PENDING, TransactionStatus.PROCESSING, TransactionStatus.PROCESSED), path);
    }

    @Test
    void retryExhaustion_marksFailed_hasNoSideEffects_andDoesNotBlockSuccessors() {
        downstream.failAlways("TXN-1");
        run(new InMemorySource()
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "100", "USD")
                .add("TXN-2", "REQ-2", "2", "ACC-2", "CREDIT", "50", "USD"));

        assertEquals(TransactionStatus.FAILED, status("TXN-1"));
        assertEquals(3, processor.find("TXN-1").orElseThrow().attempts());
        assertFalse(downstream.hasEffectFor("TXN-1"));
        assertEquals(0, bd("0").compareTo(ledger.balanceOf("ACC-1")));
        assertEquals(TransactionStatus.PROCESSED, status("TXN-2"));
        assertEquals(1, processor.summary().count(TransactionStatus.FAILED));
    }

    @Test
    void replayOfFailedTransaction_isDuplicate_notReprocessed() {
        downstream.failAlways("TXN-1");
        run(new InMemorySource()
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "100", "USD")
                .add("TXN-1", "REQ-1b", "1", "ACC-1", "CREDIT", "100", "USD"));

        assertEquals(3, downstream.totalCalls());           // no extra attempts triggered by the replay
        assertEquals(1, processor.summary().count(TransactionStatus.DUPLICATE));
    }

    @Test
    void businessRuleFailure_isPermanent_notRetried() {
        run(new InMemorySource().add("TXN-1", "REQ-1", "1", "ACC-1", "DEBIT", "10", "USD")); // no funds

        assertEquals(TransactionStatus.FAILED, status("TXN-1"));
        assertEquals(1, processor.find("TXN-1").orElseThrow().attempts());
        assertEquals(0, downstream.totalCalls());           // rules checked before any external side effect
    }

    @Test
    void currencyMismatchOnExistingAccount_failsPermanently() {
        run(new InMemorySource()
                .add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "10", "USD")
                .add("TXN-2", "REQ-2", "2", "ACC-1", "CREDIT", "10", "EUR"));

        assertEquals(TransactionStatus.FAILED, status("TXN-2"));
        assertEquals(0, bd("10").compareTo(ledger.balanceOf("ACC-1")));
    }

    @Test
    void ledgerCommitIsIdempotentEvenIfCalledTwice() {
        run(new InMemorySource().add("TXN-1", "REQ-1", "1", "ACC-1", "CREDIT", "100", "USD"));
        ledger.commit(processor.find("TXN-1").orElseThrow().request());

        assertEquals(1, ledger.entries().size());
        assertEquals(0, bd("100").compareTo(ledger.balanceOf("ACC-1")));
    }

    // ---------- observability ----------

    @Test
    void logs_maskAccountIds_andCountEvents() {
        downstream.failFirst("TXN-1", 1);
        run(new InMemorySource()
                .add("TXN-1", "REQ-1", "1", "ACC-12345", "CREDIT", "10", "USD")
                .add("TXN-1", "REQ-1b", "1", "ACC-12345", "CREDIT", "10", "USD"));

        assertEquals(1, log.count("PROCESSED"));
        assertEquals(1, log.count("RETRY_PENDING"));
        assertEquals(1, log.count("DUPLICATE"));
        assertTrue(log.events().stream().noneMatch(e -> e.format().contains("ACC-12345")));
    }
}
