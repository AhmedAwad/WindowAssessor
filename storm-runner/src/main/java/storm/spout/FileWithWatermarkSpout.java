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
    private BufferedReader br;
    private boolean done = false;
    private boolean primed = false;

    private long nextId = 1L;
    private int inFlight = 0;
    public FileWithWatermarkSpout(String path) { this.path = path; }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext ctx, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.br = new BufferedReader(new FileReader(path));
        } catch (IOException e) {
            throw new RuntimeException("Failed to open input file: " + path, e);
        }
    }

    @Override
    public void nextTuple() {
        if (done) { sleepQuiet(2); return; }

        try {
            // Prime exactly once so WM tracker is never empty before first real tuple
            if (!primed) {
                collector.emit(new Values(0L, "_PRIME_", 0.0), nextId++);
                inFlight++;
                primed = true;
                return; // yield control; Storm will call again
            }

            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("*")) continue;      // skip comments/blanks

                // Expect formats like: "26,A,15" or optionally control like "W,0" -> skip
                String[] parts = line.split(",", -1);
//                if (parts.length >= 2) continue; // don't emit watermarks as data

                // Parse event
                try {
                    long ts = Long.parseLong(parts[0]);
                    String key = parts.length > 1 ? parts[1] : "";
                    double val = (parts.length > 2 && !parts[2].isEmpty()) ? Double.parseDouble(parts[2]) : 0.0;

                    collector.emit(new Values(ts, key, val), nextId++);
                    inFlight++;
                } catch (NumberFormatException nfe) {
                    // bad line -> skip quietly
                }
                return; // emitted one tuple this call; let Storm schedule us again
            }

            // EOF reached
            done = true;
            br.close();

        } catch (IOException ioe) {
            done = true;
            // optionally log
        }
    }

    @Override public void ack(Object msgId) { inFlight = Math.max(0, inFlight - 1); }
    @Override public void fail(Object msgId) { inFlight = Math.max(0, inFlight - 1); /* optionally re-emit */ }

    @Override public void declareOutputFields(OutputFieldsDeclarer d) {
        // ts (ms), key, value
        d.declare(new Fields("ts","key","value"));
    }

    private static void sleepQuiet(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }
}
