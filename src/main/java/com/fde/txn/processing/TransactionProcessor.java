package com.fde.txn.processing;

import com.fde.txn.downstream.DownstreamGateway;
import com.fde.txn.ledger.AccountLedger;
import com.fde.txn.ledger.PermanentProcessingException;
import com.fde.txn.ledger.TransientProcessingException;
import com.fde.txn.model.RawRecord;
import com.fde.txn.model.TransactionRecord;
import com.fde.txn.model.TransactionRequest;
import com.fde.txn.model.TransactionStatus;
import com.fde.txn.observability.EventLog;
import com.fde.txn.observability.Masking;
import com.fde.txn.store.TransactionStore;
import com.fde.txn.validation.RequestValidator;
import com.fde.txn.validation.ValidationResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Orchestrates one input record at a time:
 * <pre>
 * validate -> dedupe -> sequence gate -> [check rules -> downstream -> atomic ledger commit] with bounded retry
 * </pre>
 * Business rules live in {@link AccountLedger}; input handling lives in the TransactionSource. This class only
 * owns the workflow, state transitions and the idempotency decision.
 * <p>
 * Concurrency: {@link #ingest} is serialised. That models a single consumer per partition, which is also what the
 * sequence strategy requires. Scale-out = partition by accountId, one processor per partition.
 */
public class TransactionProcessor {

    private final RequestValidator validator;
    private final TransactionStore store;
    private final AccountLedger ledger;
    private final DownstreamGateway downstream;
    private final RetryPolicy retryPolicy;
    private final EventLog log;
    private final SequenceGate gate;
    private final Sleeper sleeper;

    private final List<Rejection> rejections = new ArrayList<>();
    private final List<DuplicateEvent> duplicates = new ArrayList<>();

    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    public TransactionProcessor(RequestValidator validator, TransactionStore store, AccountLedger ledger,
                                DownstreamGateway downstream, RetryPolicy retryPolicy, EventLog log,
                                long firstSequence, Sleeper sleeper) {
        this.validator = validator;
        this.store = store;
        this.ledger = ledger;
        this.downstream = downstream;
        this.retryPolicy = retryPolicy;
        this.log = log;
        this.gate = new SequenceGate(firstSequence);
        this.sleeper = sleeper;
    }

    public synchronized void ingest(RawRecord raw) {
        ValidationResult v = validator.validate(raw);
        if (!v.valid()) {
            // Isolate the bad record, but free its sequence slot so it cannot block healthy successors.
            reject(raw, raw.get("requestId"), raw.get("transactionId"), v.errors());
            v.parsedSequence().ifPresent(gate::resolve);
            drain();
            return;
        }

        TransactionRequest req = v.request();

        // 1. Duplicate detection: by business key (transactionId), regardless of requestId or arrival time.
        Optional<TransactionRecord> existing = store.findByTransactionId(req.transactionId());
        if (existing.isPresent()) {
            if (existing.get().request().samePayloadAs(req)) {
                duplicates.add(new DuplicateEvent(raw.lineNumber(), req.requestId(), req.transactionId()));
                log.info("DUPLICATE", attrs(req, "existingStatus", existing.get().status(),
                        "requestId", req.requestId()));
            } else {
                reject(raw, req.requestId(), req.transactionId(),
                        List.of("transactionId reused with a different payload"));
            }
            return;
        }

        // 2. An idempotency key (requestId) must not be reused for a different transaction.
        Optional<String> owner = store.transactionIdForRequest(req.requestId());
        if (owner.isPresent()) {
            reject(raw, req.requestId(), req.transactionId(), List.of("requestId already used by another transaction"));
            return;
        }

        // 3. Sequence slot must still be open.
        if (gate.isStale(req.sequenceNumber()) || gate.isBuffered(req.sequenceNumber())) {
            reject(raw, req.requestId(), req.transactionId(), List.of("sequence number already consumed"));
            return;
        }

        TransactionRecord record = new TransactionRecord(req);
        if (!store.saveIfAbsent(record, req.requestId())) { // atomic guard against a concurrent twin
            duplicates.add(new DuplicateEvent(raw.lineNumber(), req.requestId(), req.transactionId()));
            log.info("DUPLICATE", attrs(req, "requestId", req.requestId()));
            return;
        }
        log.info("RECEIVED", attrs(req, "seq", req.sequenceNumber()));

        if (gate.isNext(req.sequenceNumber())) {
            execute(record);
        } else {
            record.transitionTo(TransactionStatus.PENDING_SEQUENCE, "waiting for sequence " + gate.nextExpected());
            gate.buffer(record);
            log.info("PENDING_SEQUENCE", attrs(req, "seq", req.sequenceNumber(), "waitingFor", gate.nextExpected()));
        }
        drain();
    }

    /** Releases buffered transactions whose predecessors are all resolved. */
    private void drain() {
        Optional<TransactionRecord> ready;
        while ((ready = gate.pollReady()).isPresent()) {
            log.info("RELEASED", attrs(ready.get().request(), "seq", ready.get().request().sequenceNumber()));
            execute(ready.get());
        }
    }

    private void execute(TransactionRecord record) {
        TransactionRequest req = record.request();
        record.transitionTo(TransactionStatus.PROCESSING, "attempt starting");

        while (true) {
            record.recordAttempt();
            int attempt = record.attempts();
            try {
                ledger.check(req);              // 1. permanent business-rule failures surface before any side effect
                downstream.publish(req);        // 2. idempotent external effect (key = transactionId); may fail transiently
                ledger.commit(req);             // 3. atomic + idempotent local commit
                record.transitionTo(TransactionStatus.PROCESSED, "attempt " + attempt);
                log.info("PROCESSED", attrs(req, "attempt", attempt));
                break;
            } catch (TransientProcessingException e) {
                if (attempt >= retryPolicy.maxAttempts()) {
                    record.transitionTo(TransactionStatus.FAILED, "retries exhausted after " + attempt + " attempts");
                    log.error("FAILED", attrs(req, "reason", "retries_exhausted", "attempts", attempt));
                    break;
                }
                record.transitionTo(TransactionStatus.RETRY_PENDING, "transient failure on attempt " + attempt);
                long delay = retryPolicy.delayBeforeRetry(attempt);
                log.warn("RETRY_PENDING", attrs(req, "attempt", attempt, "nextDelayMs", delay, "cause", e.getMessage()));
                if (!pause(delay)) {
                    record.transitionTo(TransactionStatus.FAILED, "interrupted while waiting to retry");
                    log.error("FAILED", attrs(req, "reason", "interrupted"));
                    break;
                }
                record.transitionTo(TransactionStatus.PROCESSING, "retry " + attempt);
            } catch (PermanentProcessingException e) {
                record.transitionTo(TransactionStatus.FAILED, e.getMessage());
                log.error("FAILED", attrs(req, "reason", e.getMessage(), "attempts", attempt));
                break;
            } catch (RuntimeException e) {
                // Safe error handling: log the type only, never the message/payload; surface a generic reason.
                record.transitionTo(TransactionStatus.FAILED, "unexpected internal error");
                log.error("FAILED", attrs(req, "reason", "unexpected_error", "type", e.getClass().getSimpleName()));
                break;
            }
        }
        // Any terminal outcome resolves the slot. A FAILED record is parked for operator recovery and must not
        // block the stream forever (see design note for the trade-off).
        gate.resolve(req.sequenceNumber());
    }

    private boolean pause(long millis) {
        if (millis <= 0) {
            return true;
        }
        try {
            sleeper.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void reject(RawRecord raw, String requestId, String transactionId, List<String> reasons) {
        rejections.add(new Rejection(raw.lineNumber(), requestId, transactionId, List.copyOf(reasons)));
        log.warn("REJECTED", Map.of("line", raw.lineNumber(), "txnId", String.valueOf(transactionId),
                "reasons", reasons));
    }

    private static Map<String, Object> attrs(TransactionRequest req, Object... kv) {
        Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("txnId", req.transactionId());
        m.put("acct", Masking.mask(req.accountId()));
        for (int i = 0; i < kv.length; i += 2) {
            m.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return m;
    }

    // ---- read side (for reporting / tests) ----

    public synchronized ProcessingSummary summary() {
        return ProcessingSummary.from(store.all(), duplicates, rejections, gate, ledger);
    }

    public synchronized List<Rejection> rejections() { return List.copyOf(rejections); }

    public synchronized List<DuplicateEvent> duplicates() { return List.copyOf(duplicates); }

    public Optional<TransactionRecord> find(String transactionId) { return store.findByTransactionId(transactionId); }
}
