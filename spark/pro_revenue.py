
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP = "kafka:29092"
CLEAN_TOPIC = "clean-events"

CHECKPOINT_DIR = (
    "/opt/spark/sample_output/checkpoints/"
    "pro_revenue_v1"
)

OUTPUT_DIR = (
    "/opt/spark/sample_output/"
    "pro_revenue_v1"
)


def write_revenue(batch_df, batch_id):
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
        f"Saved revenue updates from batch {batch_id}",
        flush=True,
    )


def main():
    spark = (
        SparkSession.builder
        .appName("MiniclipProRevenue")
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
        StructField("purchase_value", DecimalType(18, 2)),
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
        .filter(F.col("purchase_value").isNotNull())
        .withColumn(
            "event_timestamp",
            F.timestamp_seconds(F.col("time")),
        )
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["event_id"])
    )

    revenue_by_minute = (
        purchases
        .groupBy(
            F.window("event_timestamp", "1 minute")
        )
        .agg(
            F.sum("purchase_value").alias("total_revenue")
        )
        .select(
            F.col("window.start").alias("minute_start"),
            F.col("window.end").alias("minute_end"),
            "total_revenue",
        )
    )

    query = (
        revenue_by_minute.writeStream
        .outputMode("update")
        .foreachBatch(write_revenue)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(availableNow=True)
        .start()
    )

    print("Processing purchase revenue", flush=True)

    query.awaitTermination()

    if query.exception() is not None:
        raise RuntimeError(str(query.exception()))

    print("Revenue aggregation completed", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
