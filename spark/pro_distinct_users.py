
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP = "kafka:29092"
CLEAN_TOPIC = "clean-events"

CHECKPOINT_DIR = (
    "/opt/spark/sample_output/checkpoints/"
    "pro_distinct_users_v1"
)

OUTPUT_DIR = (
    "/opt/spark/sample_output/"
    "pro_distinct_users_v1"
)


def write_active_users(batch_df, batch_id):
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
        f"Saved distinct-user updates from batch {batch_id}",
        flush=True,
    )


def main():
    spark = (
        SparkSession.builder
        .appName("MiniclipProDistinctUsers")
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
        StructField("user-id", StringType()),
        StructField("user-a", StringType()),
        StructField("user-b", StringType()),
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
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("time").isNotNull())
        .withColumn(
            "event_timestamp",
            F.timestamp_seconds(F.col("time")),
        )
    )

    single_user_events = (
        events
        .filter(
            F.col("event-type").isin(
                "init",
                "in-app-purchase",
            )
        )
        .select(
            "event_id",
            "event_timestamp",
            F.col("user-id").alias("user_id"),
        )
    )

    match_users = (
        events
        .filter(F.col("event-type") == "match")
        .select(
            "event_id",
            "event_timestamp",
            F.explode(
                F.array(
                    F.col("user-a"),
                    F.col("user-b"),
                )
            ).alias("user_id"),
        )
    )

    user_activity = (
        single_user_events
        .unionByName(match_users)
        .filter(F.col("user_id").isNotNull())
        .filter(F.length(F.trim("user_id")) > 0)
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicatesWithinWatermark(
            ["event_id", "user_id"]
        )
    )

    distinct_users_by_minute = (
        user_activity
        .groupBy(
            F.window("event_timestamp", "1 minute")
        )
        .agg(
            F.collect_set("user_id").alias(
                "unique_user_ids"
            )
        )
        .select(
            F.col("window.start").alias("minute_start"),
            F.col("window.end").alias("minute_end"),
            F.size("unique_user_ids").alias(
                "distinct_users"
            ),
        )
    )

    query = (
        distinct_users_by_minute.writeStream
        .outputMode("update")
        .foreachBatch(write_active_users)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(availableNow=True)
        .start()
    )

    print(
        "Processing distinct active users",
        flush=True,
    )

    query.awaitTermination()

    if query.exception() is not None:
        raise RuntimeError(str(query.exception()))

    print(
        "Distinct-user aggregation completed",
        flush=True,
    )

    spark.stop()


if __name__ == "__main__":
    main()
