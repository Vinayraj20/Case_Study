# Idempotent Background Transaction Processing Service (FDE L2)

A small Java 17 / Maven service that processes transaction requests which may arrive **duplicated, out of order, and
with transient failures**, and still produces exactly one business outcome per transaction.

No frameworks, no external services: in-memory persistence behind interfaces (explained below).

## Run it

Requires JDK 17+ and Maven 3.8+.

```bash
mvn test                      # 25 unit/scenario tests
mvn -q compile

# Scenario from the problem statement (TXN-2005 fails once downstream, then succeeds on retry)
java -cp target/classes com.fde.txn.App --input=samples/sample-input.csv --flaky=TXN-2005:1 --backoff-ms=100

# Extended scenario: replays, tampered replay, insufficient funds, retry exhaustion, bad rows, a sequence gap
java -cp target/classes com.fde.txn.App --input=samples/sample-extended.csv --always-fail=TXN-3006 --backoff-ms=50
```

Structured logs go to **stderr**, the processing report to **stdout**. Captured output is in `samples/`.

### Sample result (problem-statement data)

| # | Input | Outcome |
|---|-------|---------|
| 1 | TXN-2002 DEBIT 40, seq 2 (arrives first) | `PENDING_SEQUENCE`, then `PROCESSED` once seq 1 lands (funds exist by then) |
| 2 | TXN-2001 CREDIT 100, seq 1 | `PROCESSED`, releases TXN-2002 |
| 3 | TXN-2002 again (REQ-2-DUP) | `DUPLICATE`, no second effect |
| 4 | TXN-2003 CREDIT 25 | `PROCESSED` |
| 5 | TXN-2004 DEBIT -10 | `REJECTED` (validation), batch continues |
| 6 | TXN-2005 CREDIT 500 | `RETRY_PENDING` once, then `PROCESSED` (2 attempts, 1 effect) |

```
Input records: 6 | PROCESSED 4 | DUPLICATE 1 | REJECTED 1 | Retries 1
Balances: {ACC-201=85, ACC-203=500}
Downstream distinct effects: 4 (total calls incl. retries: 5)
```

## Design note

### Architecture

```
TransactionSource ──► RequestValidator ──► TransactionProcessor ──► AccountLedger  (business rules + atomic commit)
 (CSV / in-memory;      (field rules,        │  dedupe → SequenceGate  DownstreamGateway (idempotent external effect)
  swap for queue/DB)     isolates bad rows)  │  → retry loop           TransactionStore  (state + idempotency keys)
                                             └─► EventLog (structured, masked) + ProcessingSummary
```

* **Input handling is separate from business logic** (`ingest/` vs `ledger/`, `processing/`). A queue consumer would implement
  `TransactionSource`; nothing else changes.
* **Business rules** (no overdraft, one currency per account) live only in `AccountLedger`.
* **Infrastructure** (store, ledger, downstream) is behind interfaces; the in-memory versions model the semantics a
  database would provide (unique keys, atomic commit).

### Status lifecycle

```
RECEIVED ─► PROCESSING ─► PROCESSED
   │            │  ▲
   │            ▼  │
   │       RETRY_PENDING ─► FAILED (attempts exhausted)
   ▼
PENDING_SEQUENCE ─► PROCESSING (when its turn comes)
```
`PROCESSING → FAILED` is also used for permanent business failures (e.g. insufficient funds). Transitions are enforced by a
state machine (`TransactionStatus.canTransitionTo`) and every transition is kept in `TransactionRecord.history()` as an
audit trail. `DUPLICATE` and `REJECTED` are dispositions of *input records* that never create/modify a transaction; they
are counted so the report accounts for every input line.

### Idempotency and duplicate detection

* **Business key = `transactionId`.** The first time it is seen, a `TransactionRecord` is inserted with
  `saveIfAbsent` (atomic; = `INSERT … ON CONFLICT DO NOTHING` on a UNIQUE column). Any later record with the same
  `transactionId` is a `DUPLICATE` **regardless of requestId, arrival position, or whether the original is processed,
  pending, or failed**. Duplicates are never re-executed.
* **`requestId` is the retry key** and may not be reused for a different transaction (`REJECTED`).
* **Same `transactionId` but different payload** (amount/account/type/…) is *not* silently treated as a duplicate: it is
  `REJECTED` as a conflict, because applying either one blindly could be wrong.
* **Defence in depth:** the ledger commit is itself idempotent per `transactionId`, and the downstream call carries
  `transactionId` as its idempotency key. A bug or crash-replay in one layer cannot double-apply an effect.

### Out-of-order strategy: sequence-based (Option A)

`SequenceGate` applies transactions strictly in `sequenceNumber` order.
* seq == next expected → process now, then drain any buffered successors.
* seq > next expected → `PENDING_SEQUENCE` (buffered); **missing sequence numbers are reported** in the summary.
* seq already consumed → `REJECTED`.
* **Why this strategy:** for a ledger, order is semantically important (the DEBIT in the sample would fail for insufficient funds
  if processed before the CREDIT). Latest-state-wins (Option C) would be wrong here because these are additive events,
  not state overwrites.
* **A slot is resolved by any terminal outcome:** PROCESSED, FAILED, or REJECTED-invalid (if its sequence parses). This way one poison
  message can't block the stream forever.

### Retry and failure handling

* Failures are classified: `TransientProcessingException` (retry) vs `PermanentProcessingException` (fail immediately, no retry).
* Bounded retries (`maxAttempts`, default 3) with exponential backoff; state shows `RETRY_PENDING` while waiting.
* **Retry safety:** each attempt runs `check rules → downstream.publish (idempotent) → atomic ledger commit`. A transient failure
  happens before any local mutation, so nothing is partially applied, and re-running is safe by construction.
* After exhaustion: `FAILED` with a reason, listed in the report for operator recovery. Failed records
  are *parked*, not retried forever, and not blocking the stream.
* A replay of a FAILED transaction is a `DUPLICATE`; recovery is a deliberate operator action, not an accidental side effect of a replay.

### Data consistency

Balance update and ledger append happen together under one lock (a DB transaction in production). Business rules are checked before any
external effect, and re-checked inside the commit.

### Observability

* Structured `key=value` events: `RECEIVED, PENDING_SEQUENCE, RELEASED, PROCESSED, DUPLICATE, REJECTED, RETRY_PENDING, FAILED`.
* `ProcessingSummary`: counts by status, retries, missing sequences, failed/pending lists, dead-letter reasons, balances.
* Security: account ids are masked in logs (`****201`); rejection reasons name the field, never echo the raw value; unexpected
  exceptions log the class name only; no secrets/config in code; all input validated before use.

### Assumptions / trade-offs

* One global sequence per stream (matches the sample data). In production sequence is per partition key (e.g. per account).
* `REVERSAL` type is not implemented (CREDIT/DEBIT only); it would need a reference to the original transaction.
* Single-writer processing (`ingest` is serialised) for simplicity and to preserve ordering.
* In-memory state is lost on restart (explicitly allowed by the brief).

## Production hardening (next steps)

1. **Persistence:** Postgres tables `transactions`(UNIQUE transaction_id, UNIQUE request_id), `ledger_entries`(UNIQUE transaction_id),
   `sequence_cursor`. Status update + ledger write in a single DB transaction; the cursor is recovered on restart.
2. **Queue semantics:** at-least-once delivery (Kafka/SQS) is exactly why idempotency is needed. Ack only after the commit; partition by
   accountId so ordering and single-writer hold while scaling horizontally; DLQ for rejected/failed.
3. **Concurrency:** replace the `synchronized` method with partition ownership + row-level locks / optimistic versioning.
4. **Gaps:** timeout on `PENDING_SEQUENCE` (alert and/or route to manual review) so a lost message doesn't block silently.
5. **Retry:** jitter, retry budget/circuit breaker for downstream, scheduled retry worker instead of in-line sleep, and an operator API to
   re-drive `FAILED` records.
6. **Observability:** Micrometer metrics (counts by status, retry rate, pending age, lag), JSON logs with correlation id, alerts on FAILED
   > 0 and pending age.
7. **Security:** authN/Z on any ingest API, encryption at rest for PII, secrets from a vault, payload size limits.
8. **Testing:** property-based tests (random shuffles + duplicates must give the same final balances), concurrency tests, contract tests for sources.

## AI-assisted development

> *(Edit this section to reflect your actual workflow before the interview.)*
Used an AI coding assistant (Claude Code) to read the problem statement, propose the architecture, scaffold the modules and draft tests.
Everything generated was verified by: running the suite (25 tests) and both demo scenarios, checking balances by hand
(100 − 40 + 25 = 85), deliberately reviewing edge cases the assistant first got wrong or skipped, and reading each class so I can explain it.
