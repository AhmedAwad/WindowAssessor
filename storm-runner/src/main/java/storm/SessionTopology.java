package storm;

import org.apache.storm.LocalCluster;
import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import storm.bolt.PrintBolt;
import storm.bolt.SessionWindowBolt;
import storm.spout.FileWithWatermarkSpout;

public class SessionTopology {
    public static void main(String[] args) throws Exception {
        String file = args.length > 0 ? args[0] : "input.txt";
        TopologyBuilder b = new TopologyBuilder();

        b.setSpout("spout", new FileWithWatermarkSpout(file), 1);
        b.setBolt("session",
                        new SessionWindowBolt(/*gap=*/10L, /*allowedLateness=*/10L), 1)
                .shuffleGrouping("spout");
        b.setBolt("printer", new PrintBolt(), 1).shuffleGrouping("session");

        Config cfg = new Config();
        cfg.setDebug(false);
        try (LocalCluster cluster = new LocalCluster()) {
            cluster.submitTopology("session-topo", cfg, b.createTopology());
            Thread.sleep(10_000); // long enough to process bounded file
        }
    }
}
