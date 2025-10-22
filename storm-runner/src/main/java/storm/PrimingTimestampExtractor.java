package storm;

import org.apache.storm.windowing.TimestampExtractor;
import org.apache.storm.tuple.Tuple;
import java.util.concurrent.atomic.AtomicBoolean;

public class PrimingTimestampExtractor implements TimestampExtractor {
    private final String tsField;
    private final AtomicBoolean primed = new AtomicBoolean(false);

    public PrimingTimestampExtractor(String tsField) {
        this.tsField = tsField;
    }

    @Override
    public long extractTimestamp(Tuple input) {
        // read your event-time from the field (millis or logical units — same units you use elsewhere)
        long ts = ((Number) input.getValueByField(tsField)).longValue();

        // first time we see a real event, flip the switch
        if (!primed.get() && primed.compareAndSet(false, true)) {
            // return a small sentinel for THIS first tuple to seed the generator
            // (you can also return ts if you prefer; the key is to avoid the empty set)
            return 0L;
        }

        // normal path after priming
        return ts;
    }
}
