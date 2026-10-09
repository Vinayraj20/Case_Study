package com.fde.txn.ingest;

import com.fde.txn.model.RawRecord;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads a header-based CSV. Deliberately lenient: it never validates business content, it only turns lines into
 * name->value maps. Short/long rows are passed on as-is so the validator can reject them individually
 * (a bad row must never stop the batch).
 */
public class CsvFileSource implements TransactionSource {
    private final Path path;

    public CsvFileSource(Path path) {
        this.path = path;
    }

    @Override
    public List<RawRecord> read() {
        try {
            List<String> lines = Files.readAllLines(path);
            if (lines.isEmpty()) {
                return List.of();
            }
            String[] header = lines.get(0).split(",", -1);
            List<RawRecord> out = new ArrayList<>();
            for (int i = 1; i < lines.size(); i++) {
                String line = lines.get(i);
                if (line.isBlank()) {
                    continue;
                }
                String[] cols = line.split(",", -1);
                Map<String, String> fields = new LinkedHashMap<>();
                for (int c = 0; c < header.length; c++) {
                    fields.put(header[c].trim(), c < cols.length ? cols[c] : null);
                }
                if (cols.length != header.length) {
                    fields.put("_malformed", "expected " + header.length + " columns, got " + cols.length);
                }
                out.add(new RawRecord(i + 1L, fields));
            }
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read input file " + path, e);
        }
    }
}
