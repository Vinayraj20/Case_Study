package com.fde.txn.ingest;

import com.fde.txn.model.RawRecord;
import java.util.List;

/**
 * Replaceable input boundary. Today: CSV file / in-memory list. Later: a Kafka/SQS poller or a DB outbox reader
 * would implement the same contract (and ack/nack after processing).
 */
public interface TransactionSource {
    List<RawRecord> read();
}
