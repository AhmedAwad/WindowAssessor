package storm.spout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.*;

public class FileWithWatermarkSpout extends BaseRichSpout {
    private final String path;
    private SpoutOutputCollector collector;
    private Iterator<String> it;
    private boolean done = false;

    public FileWithWatermarkSpout(String path) { this.path = path; }

    @Override public void open(Map conf, TopologyContext ctx, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            List<String> lines = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(path))) {
                for (String l; (l = br.readLine()) != null; ) {
                    l = l.trim();
                    if (l.isEmpty() || l.startsWith("*")) continue;
                    lines.add(l);
                }
            }
            this.it = lines.iterator();
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    @Override public void nextTuple() {
        if (done) { try { Thread.sleep(5); } catch (InterruptedException ignored) {} return; }
        if (!it.hasNext()) {
            // final flush watermark (equiv. to Flink’s MAX_WATERMARK)
            collector.emit(new Values(Long.MAX_VALUE, "W", 0.0));
            done = true;
            return;
        }
        String line = it.next();
        String[] parts = line.split(",");
        long ts = Long.parseLong(parts[0]);
        String key = parts[1];
        double val = (parts.length > 2) ? Double.parseDouble(parts[2]) : 0.0;

        collector.emit(new Values(ts, key, val));
    }

    @Override public void declareOutputFields(OutputFieldsDeclarer d) {
        // ts (ms), key, value
        d.declare(new Fields("ts","key","value"));
    }
}
