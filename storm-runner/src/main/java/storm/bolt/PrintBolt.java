package storm.bolt;

// Dumb printer bolt
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class PrintBolt extends BaseRichBolt {
    @Override public void prepare(Map conf, TopologyContext ctx, OutputCollector collector) {}
    @Override public void execute(Tuple t) {
        if ("_PRIME_".equals(t.getStringByField("key"))) {
//            collector.ack(t);
            return; // skip synthetic tuple
        }
        long s = t.getLongByField("winStart");
        long e = t.getLongByField("winEnd");
        String k = t.getStringByField("key");
        long c = t.getLongByField("count");
        System.out.printf("(%d,%d,%s,%d)%n", s, e, k, c);
    }
    @Override public void declareOutputFields(OutputFieldsDeclarer d) {}
}
