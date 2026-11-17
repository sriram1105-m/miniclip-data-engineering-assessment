
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
    "pro_matches_by_country_v1"
)

OUTPUT_DIR = (
    "/opt/spark/sample_output/"
    "pro_matches_by_country_v1"
)


def write_matches_by_country(batch_df, batch_id):
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
        f"Saved country match updates from batch {batch_id}",
        flush=True,
    )


def main():
    spark = (
        SparkSession.builder
        .appName("MiniclipProMatchesByCountry")
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
        StructField("country", StringType()),
        StructField("user-a-country", StringType()),
        StructField("user-b-country", StringType()),
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

    matches = (
        events
        .filter(F.col("event-type") == "match")
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("time").isNotNull())
        .filter(F.col("user-a-country").isNotNull())
        .filter(F.length(F.trim("user-a-country")) > 0)
        .withColumn(
            "event_timestamp",
            F.timestamp_seconds(F.col("time")),
        )
        .withColumn(
            "match_country",
            F.col("user-a-country"),
        )
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
    )

    matches_by_country = (
        matches
        .groupBy("match_country")
        .agg(
            F.count("*").alias("match_count")
        )
        .select(
            F.col("match_country").alias("country"),
            "match_count",
        )
    )

    query = (
        matches_by_country.writeStream
        .outputMode("update")
        .foreachBatch(write_matches_by_country)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(availableNow=True)
        .start()
    )

    print(
        "Processing matches by country",
        flush=True,
    )

    query.awaitTermination()

    if query.exception() is not None:
        raise RuntimeError(str(query.exception()))

    print(
        "Matches-by-country aggregation completed",
        flush=True,
    )

    spark.stop()


if __name__ == "__main__":
    main()
