package com.fde.txn;

import com.fde.txn.downstream.SimulatedDownstreamGateway;
import com.fde.txn.ingest.CsvFileSource;
import com.fde.txn.ingest.TransactionSource;
import com.fde.txn.ledger.InMemoryAccountLedger;
import com.fde.txn.model.RawRecord;
import com.fde.txn.observability.EventLog;
import com.fde.txn.processing.ProcessingSummary;
import com.fde.txn.processing.RetryPolicy;
import com.fde.txn.processing.TransactionProcessor;
import com.fde.txn.store.InMemoryTransactionStore;
import com.fde.txn.validation.RequestValidator;
import java.nio.file.Path;

/**
 * CLI runner.
 * <pre>
 * --input=samples/sample-input.csv      (required)
 * --flaky=TXN-2005:1,TXN-9:2            first N downstream calls for the txn fail transiently
 * --always-fail=TXN-2006                downstream always fails for the txn (retry exhaustion demo)
 * --max-attempts=3 --backoff-ms=200
 * </pre>
 * Logs go to stderr, the report to stdout.
 */
public class App {
    public static void main(String[] args) {
        String input = null;
        String flaky = "";
        String alwaysFail = "";
        int maxAttempts = 3;
        long backoff = 200;
        for (String a : args) {
            if (a.startsWith("--input=")) input = a.substring(8);
            else if (a.startsWith("--flaky=")) flaky = a.substring(8);
            else if (a.startsWith("--always-fail=")) alwaysFail = a.substring(14);
            else if (a.startsWith("--max-attempts=")) maxAttempts = Integer.parseInt(a.substring(15));
            else if (a.startsWith("--backoff-ms=")) backoff = Long.parseLong(a.substring(13));
            else {
                System.err.println("Unknown argument: " + a);
                System.exit(2);
            }
        }
        if (input == null) {
            System.err.println("Usage: --input=<csv> [--flaky=TXN-1:1,...] [--always-fail=TXN-2,...] "
                    + "[--max-attempts=3] [--backoff-ms=200]");
            System.exit(2);
        }

        SimulatedDownstreamGateway downstream = new SimulatedDownstreamGateway();
        for (String f : flaky.split(",")) {
            if (!f.isBlank()) {
                String[] p = f.split(":");
                downstream.failFirst(p[0].trim(), p.length > 1 ? Integer.parseInt(p[1].trim()) : 1);
            }
        }
        for (String id : alwaysFail.split(",")) {
            if (!id.isBlank()) downstream.failAlways(id.trim());
        }

        TransactionProcessor processor = new TransactionProcessor(
                new RequestValidator(), new InMemoryTransactionStore(), new InMemoryAccountLedger(), downstream,
                new RetryPolicy(maxAttempts, backoff), EventLog.toStderr(), 1, Thread::sleep);

        TransactionSource source = new CsvFileSource(Path.of(input));
        for (RawRecord raw : source.read()) {
            processor.ingest(raw);
        }

        ProcessingSummary summary = processor.summary();
        System.out.println(summary.render());
        System.out.println("Downstream distinct effects: " + downstream.distinctEffects()
                + " (total calls incl. retries: " + downstream.totalCalls() + ")");
    }
}
