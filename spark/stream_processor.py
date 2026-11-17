
import json
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


BASE_DIR = Path("/opt/spark/work-dir")
DATA_QUALITY_DIR = BASE_DIR / "data_quality"
OUTPUT_DIR = Path("/opt/spark/sample_output")

sys.path.insert(0, str(DATA_QUALITY_DIR))

from validator import EventValidator
from transformations import apply_transformations
from player_lookup import PlayerLookup
from enrichment import enrich_event
from pending_recovery import (
    recover_pending_events,
    acknowledge_recovered_event,
)


KAFKA_BOOTSTRAP = "kafka:29092"
RAW_TOPIC = "raw-events"
CLEAN_TOPIC = "clean-events"
INVALID_TOPIC = "invalid-events"

CHECKPOINT_DIR = str(OUTPUT_DIR / "checkpoints" / "data_quality")
DATABASE_PATH = OUTPUT_DIR / "player_lookup.db"
RULES_PATH = DATA_QUALITY_DIR / "transformation_rules.json"


def load_rules():
    with RULES_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def publish_events(spark, topic, records):
    """Publish JSON records to a Kafka topic using Spark."""

    if not records:
        return

    schema = StructType([
        StructField("key", StringType(), False),
        StructField("value", StringType(), False),
    ])

    rows = [
        (
            str(record["event_id"]),
            json.dumps(record["payload"], separators=(",", ":")),
        )
        for record in records
    ]

    output_df = spark.createDataFrame(rows, schema=schema)

    (
        output_df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", topic)
        .save()
    )


def transform_valid_events(spark, validated_events, rules):
    """
    Apply configurable transformations using Spark DataFrames.

    Original event dictionaries are preserved so nested Match
    and Purchase fields are not lost.
    """

    if not validated_events:
        return {}

    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event-type", StringType(), False),
        StructField("country", StringType(), True),
        StructField("platform", StringType(), True),
    ])

    rows = []

    for item in validated_events:
        event = item["event"]

        rows.append((
            item["event_id"],
            event["event-type"],
            event.get("country"),
            event.get("platform"),
        ))

    df = spark.createDataFrame(rows, schema=schema)

    transformed_df = apply_transformations(df, rules)

    transformed_values = {
        row["event_id"]: row.asDict()
        for row in transformed_df.collect()
    }

    result = {}

    for item in validated_events:
        event_id = item["event_id"]
        event = dict(item["event"])
        values = transformed_values[event_id]

        if "country" in event:
            event["country"] = values["country"]

        if "platform" in event:
            event["platform"] = values["platform"]

        result[event_id] = event

    return result


def process_batch(batch_df, batch_id, spark, validator, rules):
    """Validate, transform, enrich and publish one microbatch."""

    rows = (
        batch_df
        .orderBy("partition", "offset")
        .collect()
    )

    if not rows:
        return

    print(
        f"\nBATCH {batch_id}: received {len(rows)} events",
        flush=True,
    )

    valid_events = []
    invalid_events = []

    for row in rows:
        event_id = (
            f"{row['topic']}:{row['partition']}:{row['offset']}"
        )

        validation = validator.validate(row["raw_json"])

        if not validation["valid"]:
            invalid_events.append({
                "event_id": event_id,
                "payload": {
                    "event_id": event_id,
                    "raw_event": row["raw_json"],
                    "errors": validation["errors"],
                },
            })
            continue

        valid_events.append({
            "event_id": event_id,
            "event": validation["event"],
        })

    transformed_events = transform_valid_events(
        spark,
        valid_events,
        rules,
    )

    clean_events = []
    pending_count = 0
    recovered_ids = []

    lookup = PlayerLookup(DATABASE_PATH)

    try:
        # Process Init events first so player information is available
        # for purchases and matches in the same microbatch.
        ordered_events = sorted(
            valid_events,
            key=lambda item: (
                item["event"]["event-type"] != "init",
                item["event_id"],
            ),
        )

        for item in ordered_events:
            event_id = item["event_id"]
            event = transformed_events[event_id]

            enrichment = enrich_event(event, lookup)

            if enrichment["resolved"]:
                enriched_event = dict(enrichment["event"])
                enriched_event["event_id"] = event_id

                clean_events.append({
                    "event_id": event_id,
                    "payload": enriched_event,
                })

            else:
                lookup.save_pending_event(
                    event_id=event_id,
                    event_json=json.dumps(event),
                    created_at=int(time.time()),
                )

                pending_count += 1

        # Retry previously unresolved events after processing
        # the current batch's Init records.
        recovered = recover_pending_events(lookup)

        published_ids = {
            record["event_id"]
            for record in clean_events
        }

        for item in recovered:
            event_id = item["event_id"]

            if event_id in published_ids:
                continue

            recovered_event = dict(item["event"])
            recovered_event["event_id"] = event_id

            clean_events.append({
                "event_id": event_id,
                "payload": recovered_event,
            })

            recovered_ids.append(event_id)

        # Publish invalid events separately so they remain auditable.
        publish_events(
            spark,
            INVALID_TOPIC,
            invalid_events,
        )

        # Publish all resolved events.
        publish_events(
            spark,
            CLEAN_TOPIC,
            clean_events,
        )

        # Remove recovered records only after Kafka publication succeeds.
        for event_id in recovered_ids:
            acknowledge_recovered_event(
                lookup,
                event_id,
            )

        print(
            f"BATCH {batch_id} COMPLETE | "
            f"clean={len(clean_events)} | "
            f"invalid={len(invalid_events)} | "
            f"new_pending={pending_count} | "
            f"recovered={len(recovered_ids)} | "
            f"remaining_pending={len(lookup.get_pending_events())}",
            flush=True,
        )

    finally:
        lookup.close()


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("MiniclipDataQualityStreaming")
        .master("local[2]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    validator = EventValidator()
    rules = load_rules()

    print("PASS: JSON schemas loaded", flush=True)
    print("PASS: Transformation rules loaded", flush=True)
    print("PASS: Player enrichment modules loaded", flush=True)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )

    events_stream = raw_stream.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("value").cast("string").alias("raw_json"),
    )

    query = (
        events_stream.writeStream
        .foreachBatch(
            lambda df, batch_id: process_batch(
                df,
                batch_id,
                spark,
                validator,
                rules,
            )
        )
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(
        "PASS: Data-quality streaming processor started",
        flush=True,
    )

    print(
        f"Reading {RAW_TOPIC} and publishing to "
        f"{CLEAN_TOPIC} / {INVALID_TOPIC}",
        flush=True,
    )

    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
