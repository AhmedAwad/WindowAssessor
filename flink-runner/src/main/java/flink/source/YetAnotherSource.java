package flink.source;

import flink.events.SimpleEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

public class YetAnotherSource implements SourceFunction<Tuple3<Long,String,Double>> {

    private boolean running=true;
    private String filePath;
    private Random random = new Random();

    public YetAnotherSource(String file)
    {
        filePath = file;
    }

    @Override
    public void run(SourceContext<Tuple3<Long, String, Double>> ctx) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            long maxTs = Long.MIN_VALUE;
            while (running && (line = reader.readLine()) != null) {
                if (line.startsWith("*")) continue;

                String[] data = line.split(",");
                long ts = Long.parseLong(data[0]);
                String key = (data.length >= 2) ? data[1] : "DUMMY";
                double temperature = (data.length == 3)
                        ? Double.parseDouble(data[2])
                        : Math.round(((random.nextGaussian()*5)+20)*100.0)/100.0;

                synchronized (ctx.getCheckpointLock()) {
                    System.out.println(String.format("Emitting record with ts %d, key %s, value %f", ts, key, temperature));
                    ctx.collectWithTimestamp(Tuple3.of(ts, key, temperature), ts);

                    Thread.sleep(500);
                    if (ts > maxTs) {
                        ctx.emitWatermark(new Watermark(ts));
                        System.out.printf("Emitting watermark %d%n", ts - 1);
                        maxTs = ts;
                    }
                }
            }
            // VERY IMPORTANT: flush remaining windows at end of bounded source
            synchronized (ctx.getCheckpointLock()) {
                ctx.emitWatermark(new Watermark(Watermark.MAX_WATERMARK.getTimestamp()));
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
