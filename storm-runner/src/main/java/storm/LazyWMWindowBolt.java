package storm;
// LazyWMWindowBolt.java
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.apache.storm.windowing.TriggerHandler;
import org.apache.storm.windowing.TriggerPolicy;
import org.apache.storm.windowing.WindowLifecycleListener;
import org.apache.storm.windowing.WindowManager;
import org.apache.storm.windowing.WaterMarkEventGenerator;
import org.apache.storm.windowing.EvictionPolicy;
import org.apache.storm.windowing.TimeEvictionPolicy;
import org.apache.storm.windowing.WatermarkTimeTriggerPolicy;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class LazyWMWindowBolt extends BaseRichBolt {
    private final long windowLen;       // same units as your "ts"
    private final long slideLen;        // same units as your "ts"
    private final int  wmIntervalMs;    // real ms
    private final int  lagMs;           // allowed lateness

    private transient OutputCollector collector;
    private transient TopologyContext ctx;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private transient WindowManager<Tuple> manager;
    private transient EvictionPolicy<Tuple, ?> eviction;
    private transient TriggerPolicy<Tuple, Long> trigger;
    private transient WaterMarkEventGenerator<Tuple> wmGen;

    public LazyWMWindowBolt(long windowLen, long slideLen, int wmIntervalMs, int lagMs) {
        this.windowLen = windowLen;
        this.slideLen = slideLen;
        this.wmIntervalMs = wmIntervalMs;
        this.lagMs = lagMs;
    }

    @Override
    public void prepare(Map<String,Object> conf, TopologyContext context, OutputCollector out) {
        this.collector = out;
        this.ctx = context;
    }

    @Override
    public void execute(Tuple input) {
        // Extract event-time from your tuple field
        long ts = ((Number) input.getValueByField("ts")).longValue();

        if (started.compareAndSet(false, true)) {
            // 1) Build WindowManager with a simple listener (3-list signature)
            WindowLifecycleListener<Tuple> listener = new WindowLifecycleListener<Tuple>() {
                @Override public void onExpiry(List<Tuple> events) { /* no-op */ }

//                @Override public void onActivation(List<Tuple> events, List<Tuple> newEvents, List<Tuple> expiredEvents) {
//                    // Minimal example: emit count; adapt to your schema if needed
//                    collector.emit(new Values(-1L, -1L, "A", events.size()));
//                }
            };
            manager = new WindowManager<>(listener);

            // 2) Eviction + trigger
            eviction = new TimeEvictionPolicy<Tuple>((int)windowLen); // Storm 2.x common ctor
            manager.setEvictionPolicy(eviction);

            // WatermarkTimeTriggerPolicy(slideMs, TriggerHandler, EvictionPolicy, WindowManager)
            trigger = new WatermarkTimeTriggerPolicy<>(
                    slideLen,
                    (TriggerHandler) manager,
                    eviction,
                    manager
            );
            manager.setTriggerPolicy(trigger);
//            manager.prepare();

            // 3) Create the WM generator and PRE-SEED it so the latest-timestamps map isn't empty
            Set<GlobalStreamId> sources = ctx.getThisSources().keySet();
            wmGen = new WaterMarkEventGenerator<>(manager, wmIntervalMs, lagMs, sources);

            // PRE-SEED: give each source a tiny timestamp (e.g., 0L) BEFORE start()
            for (GlobalStreamId s : sources) {
                wmGen.track(s, 0L);
            }
            wmGen.start();  // now the first periodic WM won't be Long.MAX_VALUE
        }

        // Per tuple: update latest ts and add to window
        GlobalStreamId gsid = new GlobalStreamId(input.getSourceComponent(), input.getSourceStreamId());
        wmGen.track(gsid, ts);
        manager.add(input, ts);

        collector.ack(input);
    }

    @Override
    public void cleanup() {
        if (wmGen != null) wmGen.shutdown();
        if (manager != null) manager.shutdown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer d) {
        d.declare(new Fields("wstart","wend","key","count"));
    }
}

