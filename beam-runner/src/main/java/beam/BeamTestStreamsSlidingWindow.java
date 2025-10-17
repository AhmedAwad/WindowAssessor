package beam;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class BeamTestStreamsSlidingWindow {

    // POJO for CSV rows like: 13,A,22
    public static class Event implements Serializable {
        public long ts;
        public String key;
        public String val;

        public Event(long ts, String key, String val) {
            this.ts = ts;
            this.key = key;
            this.val = val;
        }

        @Override
        public String toString() {
            return "Event{" + ts + "," + key + "," + val + "}";
        }
    }

    public static void main(String[] args) throws IOException {
//         String fileName = "DummyDataInorderSession.csv";
        String fileName = "DummyDataWithDelaySession.csv";

        Path csvPath = Paths.get(fileName);

        Pipeline p = Pipeline.create();

        // Build a TestStream from the CSV; advance watermark to the element ts
        // (monotonic) after each record if it exceeds the current WM.
        TestStream.Builder<Event> builder = TestStream.create(SerializableCoder.of(Event.class));

        long currentWM = Long.MIN_VALUE;

        try (BufferedReader br = Files.newBufferedReader(csvPath)) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;

                String[] parts = line.split(",");
                if (parts.length < 3) {
                    throw new IllegalArgumentException("Bad CSV line (need 3 fields): " + line);
                }

                long ts = Long.parseLong(parts[0].trim());
                String key = parts[1].trim();
                String val = parts[2].trim();

                Event e = new Event(ts, key, val);
                builder = builder.addElements(TimestampedValue.of(e, new Instant(ts)));

                // Force watermark progress after this record (to event time)
                if (ts > currentWM) {
                    builder = builder.advanceWatermarkTo(new Instant(ts));
                    currentWM = ts;
                }
            }
        }

        TestStream<Event> test = builder.advanceWatermarkToInfinity();

        // Materialize the stream
        PCollection<Event> events = p.apply("TestStreamFromCSV", test);

        // Sliding window: width 10 ms, slide 2 ms; no allowed lateness; fire on WM close
        PCollection<Event> windowed =
                events.apply(
                        "SlidingWindows10msEvery2ms",
                        Window.<Event>into(
                                        SlidingWindows.of(Duration.millis(10))
                                                .every(Duration.millis(2)))
                                .withAllowedLateness(Duration.ZERO)
                                .discardingFiredPanes());

        // Drop any rows whose key is "W" if present
        PCollection<Event> onlyData =
                windowed.apply("FilterOutW", Filter.by(e -> !"W".equalsIgnoreCase(e.key)));

        PCollection<KV<String, Event>> keyed =
                onlyData
                        .apply(
                                "KeyByCSVKey",
                                WithKeys.of((BeamTestStreamsSlidingWindow.Event e) -> e.key)
                                        .withKeyType(TypeDescriptors.strings()))
                        .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BeamTestStreamsSlidingWindow.Event.class)));

        // Count per key within each sliding window
        PCollection<KV<String, Long>> counted =
                keyed
                        .apply("ToOnes",
                                MapElements
                                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                                        .via(kv -> KV.of(kv.getKey(), 1L)))
                        .apply("Count", Sum.longsPerKey());

        // Format: (start,end,key,count)
        counted.apply(
                "Format",
                ParDo.of(new DoFn<KV<String, Long>, String>() {
                    @ProcessElement
                    public void process(ProcessContext c, org.apache.beam.sdk.transforms.windowing.BoundedWindow w) {
                        IntervalWindow iw = (IntervalWindow) w;
                        KV<String, Long> kv = c.element();
                        String out = String.format("(%d,%d,%s,%d)",
                                iw.start().getMillis(), iw.end().getMillis(), kv.getKey(), kv.getValue());
                        System.out.println(out);
                    }
                }));

        p.run().waitUntilFinish();
    }
}
