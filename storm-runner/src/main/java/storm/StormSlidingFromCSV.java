package storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.utils.Utils;
import storm.spout.FileWithWatermarkSpout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class StormSlidingFromCSV {




    /** Windowed bolt: sliding event-time window 10ms/2ms, count per key (excluding 'W'). */
    public static class SlidingCountBolt extends BaseWindowedBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(TupleWindow input) {
            // Compute counts per key for this pane (ignore key == "W")
            Map<String, Long> counts = new LinkedHashMap<>();
            System.out.println("Elements in window");
            for (Tuple t : input.get()) {
                System.out.println(t.getLongByField("ts")+", "+t.getStringByField("key"));
                String key = t.getStringByField("key");
                if (!"W".equalsIgnoreCase(key)) {
                    counts.merge(key, 1L, Long::sum);
                }
            }

            // Window start/end are event-time (derived from watermarks)
            Long start = input.getStartTimestamp(); // may be null in very old Storm versions
            Long end   = input.getEndTimestamp();

            // Emit "(start,end,key,count)" per key
            if (start == null) start = -1L;
            if (end == null) end = -1L;

            for (Map.Entry<String, Long> e : counts.entrySet()) {
                String out = String.format("(%d,%d,%s,%d)", start, end, e.getKey(), e.getValue());
//                System.out.println(out);
                collector.emit(new Values(start, end, e.getKey(), e.getValue(), out));
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("start_ms", "end_ms", "key", "count", "formatted"));
        }
    }

    /** Simple sink that just prints what it receives (redundant, but handy for wiring). */
    public static class PrintBolt implements IRichBolt {
        private OutputCollector collector;

        @Override public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }
        @Override public void execute(Tuple input) {
            System.out.println(input.getStringByField("formatted"));
            collector.ack(input);
        }
        @Override public void cleanup() {}
        @Override public void declareOutputFields(OutputFieldsDeclarer declarer) {}
        @Override public Map<String, Object> getComponentConfiguration() { return null; }
    }

    public static void main(String[] args) throws Exception {
        // CSV with lines like:
        // 13,A,22
        // 27,W,0   <-- optional: keeps advancing watermark but is excluded from counts
        final String fileName = "DummyDataWithDelaySession.csv";
//        final String fileName = "DummyDataInorderSession.csv";

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("csv-spout", new FileWithWatermarkSpout(fileName), 1);

        // Sliding event-time window: size=10 ms, slide=2 ms
        // Watermark policy:
        //   - Use tuple field "ts" as event-time
        //   - withLag(0 ms): watermark ~= max(event ts seen)
        //   - withWatermarkInterval(5 ms): generate watermarks frequently
        SlidingCountBolt slidingBolt = (SlidingCountBolt) new SlidingCountBolt()
                .withWindow(BaseWindowedBolt.Duration.of(10), BaseWindowedBolt.Duration.of(2))
                .withTimestampField("ts")
                .withLag(BaseWindowedBolt.Duration.of(0))
                .withWatermarkInterval(BaseWindowedBolt.Duration.of(200));

        builder.setBolt("sliding-count", slidingBolt, 1)
                .shuffleGrouping("csv-spout");

        builder.setBolt("printer", new PrintBolt(), 1)
                .shuffleGrouping("sliding-count");

        Config cfg = new Config();
        cfg.setDebug(false);
        // If you want super-fast watermark checks, you can also lower tick freq globally, e.g.:
        // cfg.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);

        try (LocalCluster cluster = new LocalCluster()) {
            cluster.submitTopology("storm-sliding-csv", cfg, builder.createTopology());

            // Let it run a bit; adjust as needed
            Thread.sleep(20000);

            cluster.killTopology("storm-sliding-csv");
        }
    }
}
