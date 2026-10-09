package com.fde.txn.processing;

import com.fde.txn.model.TransactionRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Out-of-order strategy (Option A, sequence-based): transactions are applied strictly in sequence order.
 * A transaction whose turn has not come is buffered; when the predecessor reaches a terminal state
 * (processed, failed, or rejected as invalid) the slot is "resolved" and the next buffered one is released.
 * <p>
 * Not thread-safe by itself; the processor serialises access.
 */
public class SequenceGate {
    private long next;
    private final TreeMap<Long, TransactionRecord> buffered = new TreeMap<>();
    private final TreeSet<Long> resolvedEarly = new TreeSet<>(); // slots resolved before their turn (e.g. invalid rows)

    public SequenceGate(long firstSequence) {
        this.next = firstSequence;
    }

    public long nextExpected() { return next; }

    public boolean isStale(long seq) { return seq < next; }

    public boolean isBuffered(long seq) { return buffered.containsKey(seq); }

    public boolean isNext(long seq) { return seq == next; }

    public void buffer(TransactionRecord record) {
        buffered.put(record.request().sequenceNumber(), record);
    }

    /** Marks a slot as done (terminal outcome). Advances the cursor over any slots already resolved. */
    public void resolve(long seq) {
        if (seq < next) {
            return;
        }
        if (seq == next) {
            next++;
        } else {
            resolvedEarly.add(seq);
        }
        while (resolvedEarly.remove(next)) {
            next++;
        }
    }

    /** Removes and returns the buffered record whose turn has come, if any. */
    public Optional<TransactionRecord> pollReady() {
        return Optional.ofNullable(buffered.remove(next));
    }

    public int bufferedCount() { return buffered.size(); }

    public List<TransactionRecord> bufferedRecords() { return new ArrayList<>(buffered.values()); }

    /** Sequence numbers we are still waiting for, blocking buffered work. */
    public List<Long> missingSequences() {
        List<Long> missing = new ArrayList<>();
        if (buffered.isEmpty()) {
            return missing;
        }
        long highest = buffered.lastKey();
        for (long s = next; s < highest; s++) {
            if (!buffered.containsKey(s) && !resolvedEarly.contains(s)) {
                missing.add(s);
            }
        }
        return missing;
    }
}
