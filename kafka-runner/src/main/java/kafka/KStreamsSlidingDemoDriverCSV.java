package kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.stream.Stream;

public class KStreamsSlidingDemoDriverCSV {

    /** Build topology: branch watermarks, sliding windows, suppress until close, log with peek. */
    static Topology buildTopology() {
        StreamsBuilder b = new StreamsBuilder();

        // Input lines as plain CSV "ts,key,val"
        KStream<String, String> raw = b.stream("input-sliding",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Parse CSV into your Event(ts,key,value). Reuses existing parser.
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

        // Observe watermarks (advances stream-time in the topology)
        watermarks.peek((k, e) ->
                System.out.println("WM ts=" + e.ts + " (watermark record observed)"));

        // Sliding/Hopping windows: size=10ms, advance=2ms, grace=0
        TimeWindows sliding =
                TimeWindows.of(Duration.ofMillis(10))
                        .advanceBy(Duration.ofMillis(2))
                        .grace(Duration.ZERO);

        // Count per key within each sliding window (exclude "W" rows by using 'data')
        KTable<Windowed<String>, Long> counts =
                events
                        .selectKey((k, e) -> e.key)
                        .mapValues(e -> 1L)
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                        .windowedBy(sliding)
                        .count(
                                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("sliding-counts")
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(Serdes.Long())
                        )
                        // Emit only when the window is closed by stream-time (i.e., watermark in your CSV)
                        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        // Log and publish results as "(start,end,key,count)"
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
                .to("output-sliding",
                        Produced.with(
                                WindowedSerdes.timeWindowedSerdeFrom(String.class, 10), // window size ms
                                Serdes.String()
                        ));

        return b.build();
    }

    public static void main(String[] args) throws IOException {
//         String fileName = "DummyDataInorderSession.csv";
        String fileName = "DummyDataWithDelaySession.csv";
        Path csv = Path.of(fileName);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sliding-demo-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"); // unused by TestDriver
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        Topology topology = buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic("input-sliding",
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

                            inputTopic.pipeInput(null, line, ts);
                            if (ts > maxTs[0]) maxTs[0] = ts;
                        });
            }

            // Final synthetic watermark to advance stream-time beyond the last window end
            if (maxTs[0] > Long.MIN_VALUE) {
                long flushTs = maxTs[0] + 1000; // > last event + window size
                inputTopic.pipeInput(null, flushTs + ",W,-", flushTs);
            }

            // Drain output
            TestOutputTopic<Windowed<String>, String> outputTopic =
                    driver.createOutputTopic("output-sliding",
                            WindowedSerdes.timeWindowedSerdeFrom(String.class, 10).deserializer(),
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
