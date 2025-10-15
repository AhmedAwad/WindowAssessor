package spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.*;

public class CsvSessionStreaming {
    public static void main(String[] args) throws Exception {
//        String watchDir = args.length > 0 ? args[0] : "file:///path/to/watch_dir";

        System.setProperty("spark.driver.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED");
        System.setProperty("spark.executor.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED");


        Path p = Paths.get("watchDir");           // <— folder under your project root
        Files.createDirectories(p);                      // make sure it exists

        String watchDir = (args.length > 0)
                ? args[0]
                : p.toAbsolutePath().toUri().toString(); // e.g. "file:///C:/.../data/watch_dir"

        System.out.println("Watching: " + watchDir);

        SparkSession spark = SparkSession.builder()
                .appName("CsvSessionStreaming")
                .master("local[1]")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.streaming.sessionWindow.merge.sessions.in.local.partition","true")
                // avoid timezone surprises when converting millis → timestamp
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        spark.streams().addListener(new org.apache.spark.sql.streaming.StreamingQueryListener() {
            @Override public void onQueryStarted(QueryStartedEvent e) {
                System.out.println("Starting the query "+ e.name());
            }
            @Override public void onQueryTerminated(QueryTerminatedEvent e) {}
            @Override public void onQueryProgress(QueryProgressEvent e) {
                System.out.println(e.progress().prettyJson());
            }
        });

//        StructType schema = new StructType()
//                .add("ts_ms", DataTypes.LongType, false)
//                .add("key",   DataTypes.StringType, false)
//                .add("value", DataTypes.DoubleType, true);

        // 1) Read CSV *as a stream* from a folder (new files = new micro-batches)
        Dataset<Row> lines = spark.readStream()
                .format("text")
//                .schema(schema)
//                .option("header", "false")
//                .option("mode", "PERMISSIVE")
                .option("maxFilesPerTrigger", 1)
                .load(watchDir);

        Dataset<Row> parsed = lines
                .withColumn("line", trim(col("value")))
                .filter(col("line").isNotNull().and(length(col("line")).gt(0)))
                .withColumn("parts", split(col("line"), ","))
                .filter(size(col("parts")).geq(2)) // need at least ts and key
                .withColumn("ts_ms", col("parts").getItem(0).cast("long"))
                .withColumn("key",   col("parts").getItem(1))
                // keep ms precision: ms -> seconds(double) -> TIMESTAMP
                .withColumn("eventTime", col("ts_ms").divide(lit(1000.0)).cast("timestamp"))
                .drop("value","line","parts");
        // 2) Build event time from epoch millis


        // 3) Apply watermark BEFORE filtering, so 'W' rows still advance time
        Dataset<Row> withWM = parsed.withWatermark("eventTime", "0 milliseconds");

        // 4) Exclude 'W' rows from the aggregation (but they already advanced WM)
        Dataset<Row> agg = withWM
                .groupBy(session_window(col("eventTime"), "10 milliseconds").alias("win"),
                        col("key"))
                .agg(sum(when(col("key").notEqual("W"), lit(1)).otherwise(lit(0))).alias("count"));

        // 5) Session window: gap=10ms; lateness controlled by withWatermark(10ms)
        Dataset<Row> shaped = agg.select(
                expr("cast(unix_micros(win.start)/1000 as bigint)").alias("session_start_ms"),
                expr("cast(unix_micros(win.end)/1000 as bigint)").alias("session_end_ms"),
                col("key"),
                col("count")
        );
        StreamingQuery q = shaped.writeStream()
                .format("console")
                .outputMode("complete")
                .option("truncate", "false")
                .trigger(Trigger.ProcessingTime("10 second"))
                .start();

        q.awaitTermination();
    }
}
