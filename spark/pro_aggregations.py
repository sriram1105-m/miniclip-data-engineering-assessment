
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = "kafka:29092"
CLEAN_TOPIC = "clean-events"

CHECKPOINT_DIR = (
    "/opt/spark/sample_output/checkpoints/"
    "pro_purchase_counts_v3"
)

OUTPUT_DIR = (
    "/opt/spark/sample_output/"
    "pro_purchase_counts_v3"
)


def write_counts(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    (
        batch_df
        .withColumn("batch_id", F.lit(batch_id))
        .write
        .mode("append")
        .parquet(OUTPUT_DIR)
    )

    print(
        f"Saved purchase-count updates from batch {batch_id}",
        flush=True,
    )


def main():
    spark = (
        SparkSession.builder
        .appName("MiniclipProPurchaseCounts")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    event_schema = StructType([
        StructField("event_id", StringType()),
        StructField("event-type", StringType()),
        StructField("time", LongType()),
    ])

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", CLEAN_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    events = (
        kafka_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                event_schema,
            ).alias("event")
        )
        .select("event.*")
    )

    purchases = (
        events
        .filter(F.col("event-type") == "in-app-purchase")
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("time").isNotNull())
        .withColumn(
            "event_timestamp",
            F.timestamp_seconds(F.col("time")),
        )
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["event_id"])
    )

    purchase_counts = (
        purchases
        .groupBy(
            F.window("event_timestamp", "1 minute")
        )
        .agg(F.count("*").alias("purchase_count"))
        .select(
            F.col("window.start").alias("minute_start"),
            F.col("window.end").alias("minute_end"),
            "purchase_count",
        )
    )

    query = (
        purchase_counts.writeStream
        .outputMode("update")
        .foreachBatch(write_counts)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(availableNow=True)
        .start()
    )

    print("Processing purchase-count updates", flush=True)

    query.awaitTermination()

    if query.exception() is not None:
        raise RuntimeError(str(query.exception()))

    print("Purchase-count aggregation completed", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
