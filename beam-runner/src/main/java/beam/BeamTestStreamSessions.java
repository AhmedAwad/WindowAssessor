    package beam;

    import java.io.BufferedReader;
    import java.io.IOException;
    import java.io.Serializable;
    import java.nio.file.Files;
    import java.nio.file.Path;
    import java.nio.file.Paths;
    import java.util.Objects;
    import java.util.stream.Stream;

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
    import org.apache.beam.sdk.transforms.windowing.Sessions;
    import org.apache.beam.sdk.transforms.windowing.Window;
    import org.apache.beam.sdk.values.KV;
    import org.apache.beam.sdk.values.PCollection;
    import org.apache.beam.sdk.values.TimestampedValue;
    import org.apache.beam.sdk.values.TypeDescriptors;
    import org.joda.time.Duration;
    import org.joda.time.Instant;

    public class BeamTestStreamSessions {

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
//            String fileName = "DummyDataInorderSession.csv";
            String fileName = "DummyDataWithDelaySession.csv";

            Path csvPath = Paths.get(fileName);

            Pipeline p = Pipeline.create();

            // Build a TestStream by reading the CSV in driver code.
            // Policy:
            //  - For normal rows (key != "W"): emit the element at timestamp ts,
            //    then advance watermark to max(maxEventTsSoFar, lastExplicitWM).
            //  - For watermark rows (key == "W"): advance watermark to ts.
            TestStream.Builder<Event> builder = TestStream.create(SerializableCoder.of(Event.class));

            long currentWM = Long.MIN_VALUE;   // last watermark we emitted
            long maxEventTs = Long.MIN_VALUE;  // max event ts seen so far (excluding 'W' rows)

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

                    // Normal event
                    Event e = new Event(ts, key, val);
                    builder = builder.addElements(TimestampedValue.of(e, new Instant(ts)));

    //                if ("W".equalsIgnoreCase(key)) {
    //                     Explicit watermark instruction
                    if (ts > currentWM) {
                        builder = builder.advanceWatermarkTo(new Instant(ts));
                        currentWM = ts;
    //                    }
    //                    continue; // no element emitted
                    }



//                        // Force watermark progress after this record
//                        if (ts > maxEventTs) maxEventTs = ts;
//                        long desiredWM = Math.max(currentWM, maxEventTs);
//                        if (desiredWM > currentWM) {
//                            builder = builder.advanceWatermarkTo(new Instant(desiredWM));
//                            currentWM = desiredWM;
//                        }
                    }
                }

                TestStream<Event> test = builder.advanceWatermarkToInfinity();

                // Materialize the stream
                var events = p.apply("TestStreamFromCSV", test);

                // Window: sessions with 10 ms gap; no allowed lateness; emit when closed
                var windowed =
                        events.apply(
                                "Sessionize",
                                Window.<Event>into(Sessions.withGapDuration(Duration.millis(10)))
                                        .withAllowedLateness(Duration.ZERO)
                                        .discardingFiredPanes());

                // Drop any rows whose key might be "W" if they slipped through (defensive)
                var onlyAorOthers = windowed.apply("FilterOutW", Filter.by(e -> !"W".equalsIgnoreCase(e.key)));

                PCollection<KV<String, Event>> keyed =
                        onlyAorOthers
                                .apply(
                                        "KeyByCSVKey",
                                        WithKeys.of((BeamTestStreamSessions.Event e) -> e.key)
                                                .withKeyType(TypeDescriptors.strings()))
                                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(BeamTestStreamSessions.Event.class)));


                // Count per key within each session
                var counted =
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

