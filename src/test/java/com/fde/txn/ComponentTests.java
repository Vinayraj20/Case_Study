package com.fde.txn;

import static org.junit.jupiter.api.Assertions.*;

import com.fde.txn.ingest.CsvFileSource;
import com.fde.txn.model.RawRecord;
import com.fde.txn.model.TransactionStatus;
import com.fde.txn.processing.SequenceGate;
import com.fde.txn.validation.RequestValidator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ComponentTests {

    @Test
    void csvSource_parsesRows_andFlagsShortRows(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("in.csv");
        Files.writeString(f, "transactionId,requestId,sequence,accountId,type,amount,currency\n"
                + "T1,R1,1,A1,CREDIT,10,USD\n\nT2,R2\n");

        List<RawRecord> rows = new CsvFileSource(f).read();

        assertEquals(2, rows.size());
        assertEquals("T1", rows.get(0).get("transactionId"));
        assertNull(rows.get(0).get("_malformed"));
        assertNotNull(rows.get(1).get("_malformed"));
        assertFalse(new RequestValidator().validate(rows.get(1)).valid());
    }

    @Test
    void validator_acceptsGoodRecord_andExposesSequenceOfBadOne() {
        RequestValidator v = new RequestValidator();
        assertTrue(v.validate(new RawRecord(1, Map.of("transactionId", "T1", "requestId", "R1", "sequence", "1",
                "accountId", "A1", "type", "credit", "amount", "10.50", "currency", "USD"))).valid());

        var bad = v.validate(new RawRecord(2, Map.of("transactionId", "T1", "requestId", "R1", "sequence", "7",
                "accountId", "A1", "type", "CREDIT", "amount", "0", "currency", "USD")));
        assertFalse(bad.valid());
        assertEquals(7, bad.parsedSequence().getAsLong());
    }

    @Test
    void sequenceGate_resolvesEarlySlots_andReportsGaps() {
        SequenceGate gate = new SequenceGate(1);
        gate.resolve(3);                 // resolved ahead of time
        gate.resolve(1);
        assertEquals(2, gate.nextExpected());
        gate.resolve(2);
        assertEquals(4, gate.nextExpected()); // 3 was already resolved, so the cursor jumps over it
    }

    @Test
    void statusMachine_forbidsIllegalTransitions() {
        assertTrue(TransactionStatus.PROCESSING.canTransitionTo(TransactionStatus.RETRY_PENDING));
        assertFalse(TransactionStatus.PROCESSED.canTransitionTo(TransactionStatus.PROCESSING));
        assertFalse(TransactionStatus.FAILED.canTransitionTo(TransactionStatus.PROCESSED));
        assertTrue(TransactionStatus.PROCESSED.isTerminal());
    }
}
