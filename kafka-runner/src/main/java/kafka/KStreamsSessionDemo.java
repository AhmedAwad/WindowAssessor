package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Duration;
import java.util.Properties;

public class KStreamsSessionDemo {

    // CSV: ts_ms,key,value
    static final class CsvTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            String line = (String) record.value();
            if (line == null) return partitionTime;
            String[] parts = line.split(",");
            try {
                return Long.parseLong(parts[0].trim()); // use event time from CSV
            } catch (Exception e) {
                return partitionTime;
            }
        }
    }

    static final class Event {
        final long ts;
        final String key;
        final String value;
        Event(long ts, String key, String value) { this.ts = ts; this.key = key; this.value = value; }
        static Event parse(String line) {
            String[] p = line.split(",");
            long ts = Long.parseLong(p[0].trim());
            String k = p[1].trim();
            String v = p.length > 2 ? p[2].trim() : "";
            return new Event(ts, k, v);
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "session-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Faster, more immediate results while experimenting:
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        StreamsBuilder b = new StreamsBuilder();

        // Source: one topic with mixed A/W records; ensure single partition for determinism
        Consumed<String, String> consumed = Consumed.with(Serdes.String(), Serdes.String(),
                new CsvTimestampExtractor(), Topology.AutoOffsetReset.EARLIEST);

        KStream<String, String> raw = b.stream("input-session", consumed);

        // Parse to Event. NOTE: Stream time advances on *all* records here (including 'W').
        KStream<String, Event> events = raw.mapValues(Event::parse);

        // Filter out watermark markers *from the aggregation*, but they already advanced stream-time.
        KStream<String, Event> onlyA = events
                .filter((k, e) -> !"W".equals(e.key))
                // set key to the CSV key ('A', or whatever key domain you use)
                .selectKey((k, e) -> e.key);

        TimeWindows dummy = null; // (unused; just for illustration)

        // Session windows: gap = 10 ms, grace (allowed lateness) = 0
        SessionWindows sessions = SessionWindows.with(Duration.ofMillis(10)).grace(Duration.ZERO);

        KTable<Windowed<String>, Long> counts = onlyA
                .mapValues( e -> 1L)
                .groupByKey(Grouped.with(Serdes.String(), /* value */ Serdes.Long()))
                .windowedBy(sessions)
                .count(Materialized.as("session-counts"))
                // Emit only when the session is CLOSED (i.e., stream-time passed end + grace)
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        // Format output with start/end times
        counts.toStream()
                .mapValues((readOnlyKey, count) -> {
                    long start = readOnlyKey.window().startTime().toEpochMilli();
                    long end   = readOnlyKey.window().endTime().toEpochMilli();
                    String k   = readOnlyKey.key();
                    return String.format("(%d,%d,%s,%d)", start, end, k, count);
                })
                .to("output-session", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));

        KafkaStreams app = new KafkaStreams(b.build(), props);
        app.start();

        Runtime.getRuntime().addShutdownHook(new Thread(app::close));
    }
}
