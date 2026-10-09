package com.fde.txn.observability;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Minimal structured (key=value) event log. Keeps events in memory for the report/tests and forwards each line to a
 * sink (stderr by default). Swap for SLF4J + JSON encoder in production; callers never build log strings by hand.
 */
public class EventLog {
    public record Event(Instant at, String level, String name, Map<String, Object> attrs) {
        public String format() {
            StringBuilder sb = new StringBuilder();
            sb.append(at).append(' ').append(level).append(" event=").append(name);
            attrs.forEach((k, v) -> sb.append(' ').append(k).append('=').append(v));
            return sb.toString();
        }
    }

    private final List<Event> events = new CopyOnWriteArrayList<>();
    private final Consumer<String> sink;

    public EventLog(Consumer<String> sink) {
        this.sink = sink;
    }

    public static EventLog toStderr() {
        return new EventLog(System.err::println);
    }

    public static EventLog silent() {
        return new EventLog(line -> {});
    }

    public void info(String name, Map<String, Object> attrs) { log("INFO", name, attrs); }
    public void warn(String name, Map<String, Object> attrs) { log("WARN", name, attrs); }
    public void error(String name, Map<String, Object> attrs) { log("ERROR", name, attrs); }

    private void log(String level, String name, Map<String, Object> attrs) {
        Event e = new Event(Instant.now(), level, name, attrs);
        events.add(e);
        sink.accept(e.format());
    }

    public List<Event> events() {
        return Collections.unmodifiableList(new ArrayList<>(events));
    }

    public long count(String name) {
        return events.stream().filter(e -> e.name().equals(name)).count();
    }
}
