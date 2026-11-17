# Miniclip Data Engineering Technical Assessment

This project processes game events using Python, Apache Kafka and PySpark. It covers event generation, data validation, player enrichment and streaming aggregations, with a local environment managed through Docker Compose.

The work is organised into three parts:

- **Beginner:** Generate sample events, explore the data, run SQL quality checks and calculate daily active users.
- **Semi-Pro:** Build a Kafka streaming pipeline that validates, transforms and enriches events, while keeping invalid records separate.
- **Pro:** Calculate purchase counts, revenue, distinct purchasing users, revenue by country and match counts by country using Spark Structured Streaming.

I have also included tests for malformed events, missing fields, invalid purchase amounts, checkpoint recovery and events arriving before their player's Init event.

## 1. Architecture

```text
                      Python Event Generator
                                |
                                v
                         Kafka: raw-events
                                |
                                v
                      Spark Structured Streaming
                                |
                 +--------------+--------------+
                 |                             |
                 v                             v
          Schema Validation              Invalid Events
                 |                             |
                 v                             v
          Transformations             Kafka: invalid-events
                 |
                 v
          Player Enrichment
                 |
          +------+------+
          |             |
          v             v
     Player Lookup   Pending Events
       (SQLite)           |
                          |
                   Late Init Recovery
                          |
          +---------------+
          |
          v
    Kafka: clean-events
          |
          v
    Streaming Aggregations
          |
          +--> Purchases per minute
          +--> Revenue per minute
          +--> Distinct purchasing users
          +--> Revenue by country
          +--> Matches by country
          |
          v
       Parquet
```

Kafka separates event generation from processing. Spark reads the raw events, checks them against the relevant JSON schemas, applies the configured transformations and enriches them with player information.

Valid events are published to `clean-events`. Events that fail validation are published to `invalid-events`, together with their original payload and validation errors.

The aggregation jobs consume the clean topic and write their results to Parquet.

## 2. Technologies

| Component | Technology |
|---|---|
| Event generation | Python |
| Message broker | Apache Kafka 3.9.1 |
| Processing | Apache Spark 3.5.6, PySpark |
| Streaming | Spark Structured Streaming |
| Validation | JSON Schema, jsonschema |
| Player lookup | SQLite |
| Analytics | Spark SQL |
| Output storage | Parquet |
| Local environment | Docker Compose |

The environment uses a single Kafka broker and a local Spark container. It is intended for development and testing rather than production deployment.

## 3. Project Structure

```text
data_quality/
    enrichment.py
    pending_recovery.py
    player_lookup.py
    transformations.py
    transformation_rules.json
    validator.py

producer/
    generate_events.py
    publish_events.py
    verify_large_matches.py
    verify_large_revenue.py

schemas/
    init.json
    match.json
    in-app-purchase.json

spark/
    daily_active_users.py
    explore_events.py
    run_sql.py
    stream_processor.py
    stream_processor_large.py

    pro_aggregations.py
    pro_revenue.py
    pro_distinct_users.py
    pro_revenue_by_country.py
    pro_matches_by_country.py

    test_enrichment.py
    test_late_init.py
    test_pending_events.py
    test_pending_recovery.py
    test_player_lookup.py
    test_transformations.py
    test_validator.py

    verify_daily_active_users.py
    verify_kafka_connector.py
    verify_pro_purchase_counts.py

sql/
    01_data_profiling.sql
    02_data_quality.sql
    03_daily_active_users.sql

sample_output/
    events.jsonl
    events_large.jsonl
    checkpoints/
    Parquet outputs
    SQLite databases

docker-compose.yml
README.md
```

The top-level `schemas` and `data_quality` directories are mounted into the Spark container. The workspace also contains copies under `spark/`, but the mounted top-level directories are the ones used by the Docker configuration.

Generated datasets, checkpoints and local database files are kept under `sample_output`.

## 4. Getting Started

### Prerequisites

- Docker Desktop with Docker Compose
- Python 3
- PowerShell or a compatible terminal
- An internet connection for the initial Docker image and Spark package downloads

The Docker configuration allocates up to 1 GB of memory to Kafka and 2 GB to Spark.

### Start the containers

From the project root, run:

```powershell
docker compose up -d
```

Check that both containers are running:

```powershell
docker compose ps
```

The two containers are:

- `miniclip-kafka`
- `miniclip-spark`

Kafka is available at `localhost:9092` from the host machine and `kafka:29092` from the Spark container.

The Spark container needs the Python `jsonschema` package for validation. The streaming jobs also use the Spark Kafka connector:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6
```

The Kafka connector is supplied through the `--packages` option when running the streaming jobs.

### Kafka topics

The main pipeline uses three topics:

```text
raw-events
clean-events
invalid-events
```

To create a topic, use the following command, replacing the topic name as required:

```powershell
docker exec miniclip-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic raw-events --partitions 1 --replication-factor 1
```

Repeat this for `clean-events` and `invalid-events`.

The larger test uses a separate set of topics:

```text
raw-events-large
clean-events-large
invalid-events-large
```

Using separate topics allowed me to test a larger dataset without changing the results of the original sample.

## 5. Beginner: Event Generation and Batch Analysis

### Generate sample events

The generator produces three types of game events:

| Event type | Description |
|---|---|
| `init` | A player starts a session and provides information such as country and platform |
| `match` | A player participates in a match |
| `in-app-purchase` | A player purchases an in-game product |

Generate the initial dataset:

```powershell
python producer/generate_events.py --players 10 --output sample_output/events.jsonl
```

The dataset used during development contained:

| Event type | Count |
|---|---:|
| Init | 10 |
| Match | 5 |
| In-app purchase | 10 |
| **Total** | **25** |

### Data exploration

The batch analysis is implemented in:

```text
spark/explore_events.py
spark/run_sql.py
```

The SQL queries are stored separately:

```text
sql/01_data_profiling.sql
sql/02_data_quality.sql
sql/03_daily_active_users.sql
```

The profiling queries examine event types and player activity.

The quality checks look for missing required values, duplicate events, invalid purchase amounts and events referencing players without an Init record.

The original sample passed these checks.

### Daily active users

Daily active users are calculated by counting distinct players with Init activity for each day, country and platform.

The implementation is in:

```text
spark/daily_active_users.py
```

The result is written to Parquet and checked using:

```text
spark/verify_daily_active_users.py
```

The initial verification returned six grouped rows, representing ten distinct active players in total.

## 6. Semi-Pro: Streaming Data Quality Pipeline

The main processor is:

```text
spark/stream_processor.py
```

It consumes events from `raw-events` using Spark Structured Streaming.

Each micro-batch goes through validation, transformation and player enrichment before the records are published to the appropriate Kafka topic.

### Schema validation

The three event schemas are stored in:

```text
schemas/init.json
schemas/match.json
schemas/in-app-purchase.json
```

The validation logic is implemented in:

```text
data_quality/validator.py
```

Each event is checked against the schema for its event type.

Invalid records are not silently discarded. The processor retains the original event and records why validation failed.

For example, an Init event without a country produces an error similar to:

```json
{
  "event_id": "raw-events:0:25",
  "raw_event": "{\"event-type\":\"init\",\"time\":123,\"user-id\":\"invalid_test_001\",\"platform\":\"Android\"}",
  "errors": [
    {
      "field": "country",
      "message": "'country' is a required property"
    }
  ]
}
```

The event identifier is derived from the Kafka topic, partition and offset. This makes it possible to trace an output record back to its source position.

### Transformations

Transformation rules are defined in:

```text
data_quality/transformation_rules.json
```

The implementation is in:

```text
data_quality/transformations.py
```

Keeping the rules in a separate configuration file makes them easier to review without changing the main streaming processor.

One of the transformations standardises platform values. During testing, `Android` was correctly transformed to `ANDROID`.

### Player enrichment

Match and purchase events need player information that may not be present in the event itself.

The pipeline uses SQLite to maintain a lookup of players from Init events.

The relevant modules are:

```text
data_quality/player_lookup.py
data_quality/enrichment.py
data_quality/pending_recovery.py
```

When a match or purchase arrives, the processor looks up the player and adds the available attributes, including country.

The main complication is that events do not always arrive in the expected order.

A purchase might reach Kafka before the player's Init event. Publishing it immediately would leave the country missing, while rejecting it would discard an otherwise valid purchase.

I chose to keep these events in a pending table until the player information becomes available.

When the Init event arrives, the processor checks the pending records, enriches the matching events and publishes them to the clean topic.

This behaviour was tested with a purchase arriving before its Init event.

### Checkpointing

Spark checkpoints are stored under:

```text
sample_output/checkpoints/
```

The processor uses its checkpoint to resume from previously committed Kafka offsets.

I tested this by restarting the processor with its existing checkpoint. The original clean records were not published again during that restart.

This is useful for recovery, although it does not provide a complete exactly-once guarantee. Kafka publishing, SQLite updates and Spark checkpoint commits are separate operations.

## 7. Pro: Streaming Aggregations

The Pro section contains five streaming aggregation jobs.

| File | Calculation |
|---|---|
| `pro_aggregations.py` | Number of purchases per minute |
| `pro_revenue.py` | Revenue per minute |
| `pro_distinct_users.py` | Distinct purchasing users per minute |
| `pro_revenue_by_country.py` | Purchase revenue by country |
| `pro_matches_by_country.py` | Match counts by country |

The jobs consume the clean Kafka topic and write their results to Parquet.

### Event time and deduplication

The aggregations use event time rather than relying solely on the time an event reaches the processor.

Watermarks provide a defined allowance for late events, while event identifiers are used for deduplication.

There is a trade-off here. A longer watermark allowance can accommodate more late events, but it also requires the streaming job to retain state for longer.

### Revenue calculations

Revenue is calculated using decimal values rather than relying on floating-point arithmetic for financial totals.

This avoids avoidable rounding errors when purchase values are aggregated.

### Verified results

The original sample contained ten purchase events.

I then injected one additional purchase directly into `clean-events` to test the Pro aggregations.

The resulting checks confirmed:

| Metric | Result |
|---|---:|
| Purchases | 11 |
| Total revenue | 268.23 |
| Matches | 5 |

The country-level revenue output contained five countries, with a combined revenue of 268.23.

The match aggregation contained three countries and five matches in total.

The purchase-count aggregation was also restarted with its existing checkpoint. With no new events, it did not produce duplicate output during that test.

### Handling updated aggregation results

Some of the streaming jobs write successive versions of aggregation results to Parquet, with a `batch_id` identifying the update.

These files represent a history of updates, not a table where every row should be added together.

When reading the output, the latest batch must be selected for each aggregation key.

For a production system, I would prefer a sink with reliable upsert support, such as a transactional table format, rather than managing update versions in append-only Parquet files.

## 8. Larger End-to-End Test

After verifying the initial pipeline, I generated a larger dataset to check how the processing behaved with more players and events.

The dataset was created with:

```powershell
python producer/generate_events.py --players 400 --seed 123 --output sample_output/events_large.jsonl
```

It contained:

| Event type | Count |
|---|---:|
| Init | 400 |
| Match | 200 |
| In-app purchase | 400 |
| **Total** | **1,000** |

There were 400 unique players.

### Publish the dataset

```powershell
python producer/publish_events.py --input sample_output/events_large.jsonl --topic raw-events-large --bootstrap-servers localhost:9092 --delay 0
```

The larger dataset uses a separate processor:

```text
spark/stream_processor_large.py
```

This is a wrapper around the main processor with separate Kafka topics, a separate checkpoint and a separate SQLite database.

Run it with:

```powershell
docker exec miniclip-spark /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 512m --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 /opt/spark/work-dir/stream_processor_large.py
```

### Results

The processor completed the initial 1,000-event batch with:

```text
BATCH 0: received 1000 events
BATCH 0 COMPLETE | clean=1000 | invalid=0 | new_pending=0 | recovered=0 | remaining_pending=0
```

I checked the Kafka topic offsets, SQLite player records and the contents of the clean topic.

The results were:

| Check | Result |
|---|---:|
| Raw events | 1,000 |
| Clean events | 1,000 |
| Invalid events | 0 |
| Player records | 400 |
| Pending events | 0 |
| Unique clean event IDs | 1,000 |
| Purchases | 400 |
| Matches | 200 |
| Total revenue | 9,632.10 |

The event counts and revenue were checked independently against the generated source data.

### Revenue by country

| Country | Purchases | Revenue |
|---|---:|---:|
| Australia | 75 | 1,896.99 |
| Germany | 52 | 1,323.55 |
| India | 65 | 1,365.32 |
| Portugal | 67 | 1,608.65 |
| Spain | 63 | 1,675.49 |
| United Kingdom | 78 | 1,762.10 |
| **Total** | **400** | **9,632.10** |

The verification script is:

```text
producer/verify_large_revenue.py
```

To check the original 1,000 clean events:

```powershell
docker exec miniclip-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic clean-events-large --from-beginning --max-messages 1000 2>$null | python producer/verify_large_revenue.py
```

### Matches by country

| Country | Matches |
|---|---:|
| Australia | 37 |
| Germany | 23 |
| India | 31 |
| Portugal | 38 |
| Spain | 30 |
| United Kingdom | 41 |
| **Total** | **200** |

The verification script is:

```text
producer/verify_large_matches.py
```

Run it with:

```powershell
docker exec miniclip-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic clean-events-large --from-beginning --max-messages 1000 2>$null | python producer/verify_large_matches.py
```

These results relate to the original 1,000-event dataset, before the additional edge-case tests.

The larger test exercised the data-quality streaming pipeline and independently checked the analytical totals from Kafka. I did not run all five Pro streaming aggregation jobs against the larger topics.

## 9. Edge-Case Testing

I used the larger Kafka topics for additional tests so that the original sample and its aggregation outputs remained unchanged.

### Malformed JSON

I published an incomplete JSON message.

The processor rejected it and published the original payload to `invalid-events-large` with:

```json
"errors": ["Malformed JSON"]
```

### Missing required field

I published an Init event without `country`.

The processor rejected it with:

```json
{
  "field": "country",
  "message": "'country' is a required property"
}
```

### Negative purchase value

I published a purchase with `purchase_value` set to `-10.00`.

The event was rejected with:

```json
{
  "field": "purchase_value",
  "message": "-10.0 is less than the minimum of 0"
}
```

### Purchase arriving before Init

I published a valid purchase for a player who did not yet exist in the lookup database.

The processor reported:

```text
BATCH 4 COMPLETE | clean=0 | invalid=0 | new_pending=1 | recovered=0 | remaining_pending=1
```

This confirmed that the purchase was held rather than rejected or published without enrichment.

I then published the player's Init event with `country` set to `Portugal`.

The next batch reported:

```text
BATCH 5 COMPLETE | clean=2 | invalid=0 | new_pending=0 | recovered=1 | remaining_pending=0
```

The two clean records were inspected directly in Kafka.

The recovered purchase contained:

```json
{
  "event-type": "in-app-purchase",
  "time": 1794898002,
  "user-id": "edge_unknown_player_001",
  "product-id": "coins_100",
  "purchase_value": 15.0,
  "country": "Portugal",
  "event_id": "raw-events-large:0:1003"
}
```

The Init event contained:

```json
{
  "event-type": "init",
  "time": 1794898003,
  "user-id": "edge_unknown_player_001",
  "country": "Portugal",
  "platform": "ANDROID",
  "event_id": "raw-events-large:0:1004"
}
```

This confirmed that the purchase was recovered, its original event ID was retained and the country was correctly added.

The recovered purchase appeared before the Init event in the clean topic. This reflects the processor's publishing order and means downstream consumers should not assume that Init events always appear first.

### Final Kafka offsets

After the edge-case tests:

```text
clean-events-large:0:1002
invalid-events-large:0:3
```

The clean topic contained the original 1,000 events, the recovered purchase and the late Init event.

The invalid topic contained the three deliberately rejected records.

## 10. Testing

The project includes component tests for validation, transformations, player lookup, enrichment and pending-event recovery.

These are located in the `spark/` directory.

The testing covered:

- Schema validation for different event types.
- Transformation rules.
- Player lookup and enrichment.
- Pending-event storage and recovery.
- Daily active user calculations.
- Purchase and revenue aggregations.
- Kafka topic contents and offsets.
- Checkpoint restart behaviour.
- Larger-volume event processing.
- Invalid events and out-of-order arrival.

The tests were run during development, and their outputs were checked before moving to the next part of the implementation.

The checkpoint tests covered normal restarts and confirmed the observed output behaviour. They did not simulate a crash at every possible point in a micro-batch.

## 11. Design Decisions

### Keeping validation separate from processing

I kept the schemas, validation logic and transformation rules outside the main streaming processor.

This makes it easier to change the accepted event structure or a standardisation rule without rewriting the rest of the pipeline.

It also allows the individual components to be tested separately.

### Retaining invalid events

I chose to publish rejected events to a separate Kafka topic rather than discard them.

Keeping the original payload and validation error makes it easier to investigate data quality problems and provides a starting point for reprocessing if the upstream data is corrected.

### Handling missing player information

I considered rejecting events that arrived before Init, but that would lose valid activity simply because the events arrived out of order.

The pending-event approach keeps these records until the missing player information is available.

SQLite was sufficient for this local assessment because the lookup and pending state could be inspected directly.

For a distributed production system, I would move this state to a more suitable shared or streaming state store.

### Using Kafka offsets for event identity

Each source event receives an identifier based on its topic, partition and offset.

This gives the processor a stable identifier when it reads the same Kafka record again and allows downstream jobs to recognise repeated input.

It is not a substitute for a business-level event identifier if the same logical event is published to Kafka more than once at different offsets.

### Separating ingestion and aggregation

The data-quality processor publishes clean events to Kafka rather than calculating every metric itself.

This allows the aggregation jobs to consume a common validated stream without duplicating the validation and enrichment logic.

It also means that changes to an analytical calculation do not necessarily require changes to ingestion.

### Using Parquet for analytical outputs

Parquet is convenient for local analytical processing and can be read directly by Spark.

However, the update-mode aggregation jobs need to retain successive versions of their results.

For a production system with frequent updates, I would use a transactional table format or another sink that supports reliable upserts.

## 12. Limitations and Improvements

The pipeline works for the scenarios tested, but there are several areas I would address before using this design in production.

### Driver-side processing

The data-quality processor collects each micro-batch to the Spark driver.

This keeps the validation and enrichment flow straightforward, but it limits how far the application can scale.

For larger workloads, I would move more of the processing into distributed Spark operations and avoid collecting entire batches on the driver.

### Local SQLite state

SQLite works for this assessment, but it is not suitable as a shared state store across multiple distributed Spark workers.

A production design would need a more scalable approach to player lookup and pending-event recovery.

### Delivery guarantees

Spark checkpoints track streaming progress, but publishing to Kafka, updating SQLite and committing a checkpoint are not one atomic operation.

A failure after an output has been published but before the checkpoint is committed could cause the same event to be published again.

I would address this with idempotent writes, transactional sinks where appropriate and tests that deliberately interrupt processing between these operations.

### Streaming aggregation outputs

Some aggregation jobs append new result versions to Parquet.

Consumers must select the latest version for each key. Reading all versions as independent facts would overcount the results.

A production implementation should support reliable updates without requiring this additional interpretation.

### Kafka durability

The local Kafka broker uses a single node without replication.

Its logs are stored in `/tmp/kraft-combined-logs`, which is not mounted to a persistent host volume.

Recreating the Kafka container can therefore lose the topic data.

A production deployment would require durable storage, replication and suitable retention settings.

### Processing performance

During the 1,000-event test, Spark reported that the micro-batch took longer than the configured ten-second trigger interval.

The batch completed successfully, but this warning shows that processing time and throughput would need attention as the workload grows.

I would investigate driver-side collection, repeated Spark operations, SQLite access and Kafka write overhead before adjusting the trigger interval.

### Further testing

The existing tests cover the main functionality and several important failure cases.

Additional work would include crash simulation during Kafka publishing, replay tests, larger datasets, late-event behaviour around watermark boundaries and automated integration tests.

## 13. Final Notes

The implementation covers the Beginner, Semi-Pro and Pro requirements through a combination of batch analysis, Kafka streaming, data quality checks, player enrichment and event-time aggregations.

The most useful part of the testing was the out-of-order purchase scenario. It showed why validation alone is not enough: a structurally valid event can still be missing information that arrives later.

The larger dataset also helped expose the practical limits of the current approach, particularly the time spent processing a micro-batch on the driver.

I have kept those limitations explicit because the next engineering decisions would depend on the expected event volume, latency requirements and recovery guarantees.