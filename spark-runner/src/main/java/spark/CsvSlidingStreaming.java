package spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.*;

public class CsvSlidingStreaming {
    public static void main(String[] args) throws Exception {
        // For some JDKs on Windows; harmless elsewhere
        System.setProperty("spark.driver.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED");
        System.setProperty("spark.executor.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED");

        Path p = Paths.get("watchDir"); // folder under your project root
        Files.createDirectories(p);

        String watchDir = (args.length > 0)
                ? args[0]
                : p.toAbsolutePath().toUri().toString(); // e.g., "file:///C:/.../watchDir"

        System.out.println("Watching: " + watchDir);

        SparkSession spark = SparkSession.builder()
                .appName("CsvSlidingStreaming")
                .master("local[1]")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.session.timeZone", "UTC") // keep millis → timestamp stable
                .getOrCreate();

        spark.streams().addListener(new org.apache.spark.sql.streaming.StreamingQueryListener() {
            @Override public void onQueryStarted(QueryStartedEvent e) {
                System.out.println("Starting the query " + e.name());
            }
            @Override public void onQueryTerminated(QueryTerminatedEvent e) {}
            @Override public void onQueryProgress(QueryProgressEvent e) {
                System.out.println(e.progress().prettyJson());
            }
        });

        // 1) Read CSV lines as a stream from a folder (new files => new micro-batch)
        Dataset<Row> lines = spark.readStream()
                .format("text")
                .option("maxFilesPerTrigger", 1) // one file per trigger (like your session setup)
                .load(watchDir);

        // 2) Parse "ts,key,val" and build event-time from epoch millis (keep ms precision)
        Dataset<Row> parsed = lines
                .withColumn("line", trim(col("value")))
                .filter(col("line").isNotNull().and(length(col("line")).gt(0)))
                .withColumn("parts", split(col("line"), ","))
                .filter(size(col("parts")).geq(2))                // need at least ts and key
                .withColumn("ts_ms", col("parts").getItem(0).cast("long"))
                .withColumn("key",   col("parts").getItem(1))
                .withColumn("eventTime", col("ts_ms").divide(lit(1000.0)).cast("timestamp"))
                .drop("value","line","parts");

        // 3) Watermark first so 'W' rows (if any) still advance event-time before filtering
        Dataset<Row> withWM = parsed.withWatermark("eventTime", "0 milliseconds");

        // 4) Exclude 'W' rows from aggregation (but they already advanced the watermark)
//        Dataset<Row> dataOnly = withWM.filter(col("key").notEqual("W"));

        // 5) Sliding window: size=10 ms, slide=2 ms; lateness controlled by watermark(0 ms)
        Dataset<Row> agg = withWM
                .groupBy(
                        window(col("eventTime"), "10 milliseconds", "2 milliseconds").alias("win"),
                        col("key"))
                .count();

        // 6) Shape output: (start_ms,end_ms,key,count)
        Dataset<Row> shaped = agg.select(
                expr("cast(unix_micros(win.start)/1000 as bigint)").alias("window_start_ms"),
                expr("cast(unix_micros(win.end)/1000 as bigint)").alias("window_end_ms"),
                col("key"),
                col("count")
        );

        // 7) Append mode => emit only after window closes by watermark (your “force at WM” behavior)
        StreamingQuery q = shaped.writeStream()
                .format("console")
                .outputMode("append")
                .option("truncate", "false")
                .trigger(Trigger.ProcessingTime("10 seconds")) // check for new files every 10s
                .start();

        q.awaitTermination();
    }
}
