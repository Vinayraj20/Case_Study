package com.fde.txn.downstream;

import com.fde.txn.ledger.TransientProcessingException;
import com.fde.txn.model.TransactionRequest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Fault-injecting downstream used for the demo and tests. */
public class SimulatedDownstreamGateway implements DownstreamGateway {
    private final Map<String, Integer> failuresRemaining = new HashMap<>();
    private final Set<String> alwaysFail = new HashSet<>();
    private final Set<String> effects = new HashSet<>();
    private int calls;

    /** The first {@code times} calls for this transaction fail transiently. */
    public SimulatedDownstreamGateway failFirst(String transactionId, int times) {
        failuresRemaining.put(transactionId, times);
        return this;
    }

    /** Every call for this transaction fails transiently (to demonstrate retry exhaustion). */
    public SimulatedDownstreamGateway failAlways(String transactionId) {
        alwaysFail.add(transactionId);
        return this;
    }

    @Override
    public synchronized void publish(TransactionRequest request) {
        calls++;
        String id = request.transactionId();
        if (alwaysFail.contains(id)) {
            throw new TransientProcessingException("downstream unavailable (simulated)");
        }
        int remaining = failuresRemaining.getOrDefault(id, 0);
        if (remaining > 0) {
            failuresRemaining.put(id, remaining - 1);
            throw new TransientProcessingException("downstream timeout (simulated)");
        }
        effects.add(id); // keyed by idempotency key => a duplicate call is absorbed
    }

    public synchronized int distinctEffects() { return effects.size(); }
    public synchronized int totalCalls() { return calls; }
    public synchronized boolean hasEffectFor(String transactionId) { return effects.contains(transactionId); }
}
