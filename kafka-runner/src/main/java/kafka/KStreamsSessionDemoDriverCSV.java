package kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.stream.Stream;

public class KStreamsSessionDemoDriverCSV {

    /** Build topology: branch watermarks, sessionize data, suppress until close, log with peek. */
    static Topology buildTopology() {
        StreamsBuilder b = new StreamsBuilder();

        // We'll inject CSV lines as values from the driver
        KStream<String, String> raw = b.stream("input-session",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Parse CSV into your Event(ts,key,value)
        KStream<String, KStreamsSessionDemo.Event> events =
                raw.mapValues(KStreamsSessionDemo.Event::parse);

        // Branch: [0]=watermarks (key == "W"), [1]=everything else
        @SuppressWarnings("unchecked")
        KStream<String, KStreamsSessionDemo.Event>[] branches = events.branch(
                (k, e) -> "W".equals(e.key),
                (k, e) -> true
        );

        KStream<String, KStreamsSessionDemo.Event> watermarks = branches[0];
        KStream<String, KStreamsSessionDemo.Event> data       = branches[1];

        // Log each watermark arrival (this advances stream-time)
        watermarks.peek((k, e) ->
                System.out.println("WM ts=" + e.ts + " (watermark record observed)"));

        // Session windows (gap=10ms, grace=0)
        SessionWindows sessions = SessionWindows.with(Duration.ofMillis(10)).grace(Duration.ZERO);

        // Aggregate only real data; set key = CSV key (e.g., "A")
        KTable<Windowed<String>, Long> counts = events
                .selectKey((k, e) -> e.key)
                .mapValues(e -> 1L)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(sessions)
                .count(
                        Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("session-counts")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        // On emission, log the window and value, then write to output topic
        counts.toStream()
                .peek((wk, count) -> {
                    long start = wk.window().startTime().toEpochMilli();
                    long end   = wk.window().endTime().toEpochMilli();
                    System.out.println("EMIT start=" + start + " end=" + end +
                            " key=" + wk.key() + " count=" + count);
                })
                .mapValues((wk, count) -> {
                    long start = wk.window().startTime().toEpochMilli();
                    long end   = wk.window().endTime().toEpochMilli();
                    String k   = wk.key();
                    return String.format("(%d,%d,%s,%d)", start, end, k, count);
                })
                .to("output-session",
                        Produced.with(
                                WindowedSerdes.sessionWindowedSerdeFrom(String.class),
                                Serdes.String()
                        ));

        return b.build();
    }

    public static void main(String[] args) throws IOException {
//        if (args.length < 1) {
//            System.err.println("Usage: java ... KStreamsSessionDemoDriverCsvWithWM <path/to/events.csv>");
//            System.exit(2);
//        }
//        String fileName = "DummyDataInorderSession.csv";
        String fileName = "DummyDataWithDelaySession.csv";
        Path csv = Path.of(fileName);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "session-demo-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"); // not used by TestDriver
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        Topology topology = buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            // Create TestInputTopic instead of ConsumerRecordFactory
            TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic("input-session",
                            Serdes.String().serializer(),
                            Serdes.String().serializer());

            final long[] maxTs = {Long.MIN_VALUE};

            // Feed CSV lines (ts_ms,key,value) with event-time = ts_ms
            try (Stream<String> lines = Files.lines(csv)) {
                lines.map(String::trim)
                        .filter(s -> !s.isEmpty() && !s.startsWith("#"))
                        .forEach(line -> {
                            if (isHeader(line)) return;
                            String[] p = line.split(",", -1);
                            if (p.length < 2) return;

                            long ts;
                            try { ts = Long.parseLong(p[0].trim()); }
                            catch (Exception ignored) { return; }

                            // Use TestInputTopic.pipeInput with key, value, and timestamp
                            inputTopic.pipeInput(null, line, ts);

                            if (ts > maxTs[0]) maxTs[0] = ts;
                        });
            }

            // Add a final synthetic watermark to flush trailing sessions
            if (maxTs[0] > Long.MIN_VALUE) {
                long flushTs = maxTs[0] + 1000; // safely beyond last event + gap
                inputTopic.pipeInput(null, flushTs + ",W,-", flushTs);
            }

            // Drain and print outputs (the topology also logged EMIT via peek)
            TestOutputTopic<Windowed<String>, String> outputTopic =
                    driver.createOutputTopic("output-session",
                            WindowedSerdes.sessionWindowedSerdeFrom(String.class).deserializer(),
                            Serdes.String().deserializer());

            while (!outputTopic.isEmpty()) {
                String value = outputTopic.readValue();
                System.out.println(value);
            }
        }
    }

    private static boolean isHeader(String line) {
        String lower = line.toLowerCase();
        return lower.startsWith("ts") || lower.startsWith("timestamp");
    }
}
