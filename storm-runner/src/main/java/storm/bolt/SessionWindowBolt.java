package storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Fields;

import java.util.*;

public class SessionWindowBolt extends BaseRichBolt {
    private final long gapMs;
    private final long allowedLatenessMs;
    private OutputCollector collector;

    private static class Session {
        long start;        // inclusive
        long end;          // exclusive = lastEventTs + gap
        long count;
        boolean emitted;   // has fired at least once
        Session(long s, long e, long c) { start=s; end=e; count=c; }
    }

    // per-key active sessions (non-overlapping)
    private final Map<String, List<Session>> state = new HashMap<>();
    private long currentWM = Long.MIN_VALUE;

    public SessionWindowBolt(long gapMs, long allowedLatenessMs) {
        this.gapMs = gapMs;
        this.allowedLatenessMs = allowedLatenessMs;
    }

    @Override public void prepare(Map conf, TopologyContext ctx, OutputCollector collector) {
        this.collector = collector;
    }

    @Override public void execute(Tuple t) {
        long ts = t.getLongByField("ts");
        String key = t.getStringByField("key");
        double value = t.getDoubleByField("value");

        if ("W".equals(key)) {
            // advance watermark and handle firing & cleanup
            if (ts > currentWM) {
                currentWM = ts;
                onWatermark();
            }
            collector.ack(t);
            return;
        }

        // Process data event for 'key'
        List<Session> sessions = state.computeIfAbsent(key, k -> new ArrayList<>());
        // Build provisional session
        Session merged = new Session(ts, ts + gapMs, 1);

        // Find overlaps BEFORE acceptance check (per Flink semantics)
        List<Session> overlaps = new ArrayList<>();
        for (Session s : sessions) {
            if (overlap(merged, s)) overlaps.add(s);
        }
        // Merge ranges & counts
        for (Session s : overlaps) {
            merged.start = Math.min(merged.start, s.start);
            merged.end   = Math.max(merged.end,   s.end);
            merged.count += s.count;
            merged.emitted |= s.emitted;
        }
        // Lateness check on the MERGED end:
        // keep iff currentWM <= (merged.end - 1) + allowedLateness
        if (currentWM > (merged.end - 1) + allowedLatenessMs) {
            // too late -> drop (or send to a side stream if you like)
            collector.ack(t);
            return;
        }

        // Accept: remove overlapped, insert merged
        sessions.removeAll(overlaps);
        sessions.add(merged);

        // If it had fired earlier (emitted==true), re-fire with updated aggregate (Flink retraction/update style)
        if (merged.emitted) {
            emitWindow(merged.start, merged.end, key, merged.count);
        }
        collector.ack(t);
    }

    private boolean overlap(Session a, Session b) {
        // intervals [start, end)
        return a.start < b.end && b.start < a.end;
    }

    private void onWatermark() {
        // For each key/session, fire windows whose end <= WM; purge when WM > end + L
        for (Map.Entry<String, List<Session>> e : state.entrySet()) {
            String key = e.getKey();
            List<Session> list = e.getValue();
            Iterator<Session> it = list.iterator();
            List<Session> keep = new ArrayList<>();
            while (it.hasNext()) {
                Session s = it.next();
                if (s.end <= currentWM) {
                    emitWindow(s.start, s.end, key, s.count);
                    s.emitted = true; // keep until cleanup threshold
                }
                // cleanup if WM has passed end + allowed lateness
                if (currentWM > s.end + allowedLatenessMs) {
                    // drop from state (window is finalized)
                } else {
                    keep.add(s);
                }
            }
            e.setValue(keep);
        }
    }

    private void emitWindow(long start, long end, String key, long count) {
        collector.emit(new Values(start, end, key, count));
    }

    @Override public void declareOutputFields(OutputFieldsDeclarer d) {
        d.declare(new Fields("winStart","winEnd","key","count"));
    }
}

