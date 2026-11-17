# Sample Pipeline Results

This folder contains a sample of the generated game events and a summary of the outputs produced during local testing.

The original analytical outputs were written in Parquet format. The results below were verified against those outputs or the processed Kafka events.

## 1. Generated Events

**File:** `events.jsonl`

The initial dataset contains 25 events across 10 players:

| Event type | Count |
|---|---:|
| Init | 10 |
| Match | 5 |
| In-app purchase | 10 |
| **Total** | **25** |

## 2. Daily Active Users

The daily active users calculation groups distinct players with Init activity by date, country and platform.

The initial dataset produced six grouped rows, representing 10 distinct players.

## 3. Purchase Counts and Revenue

The Pro streaming aggregations produced the following results:

| Metric | Verified result |
|---|---:|
| Purchase events | 11 |
| Total purchase revenue | 268.23 |
| Distinct purchasing users | 10 |

The purchase-count aggregation recorded 11 purchases in the minute beginning `2026-11-15 12:51:00`.

These figures include one additional purchase event introduced during the streaming aggregation tests.

## 4. Larger Dataset Validation

A separate dataset containing 1,000 events across 400 players was processed through the data-quality pipeline.

| Metric | Verified result |
|---|---:|
| Init events | 400 |
| Match events | 200 |
| Purchase events | 400 |
| Total events | 1,000 |
| Total purchase revenue | 9,632.10 |

### Purchase Revenue by Country

| Country | Purchases | Revenue |
|---|---:|---:|
| Australia | 75 | 1,896.99 |
| Germany | 52 | 1,323.55 |
| India | 65 | 1,365.32 |
| Portugal | 67 | 1,608.65 |
| Spain | 63 | 1,675.49 |
| United Kingdom | 78 | 1,762.10 |
| **Total** | **400** | **9,632.10** |

### Match Events by Country

| Country | Matches |
|---|---:|
| Australia | 37 |
| Germany | 23 |
| India | 31 |
| Portugal | 38 |
| Spain | 30 |
| United Kingdom | 41 |
| **Total** | **200** |

These larger-dataset country totals were calculated from the processed clean Kafka events, rather than by running the separate Pro aggregation scripts against the larger topic.

## 5. Data Quality and Recovery Tests

Additional events were introduced to verify error handling and late-arriving data.

| Scenario | Result |
|---|---|
| Malformed JSON | Rejected |
| Missing required country | Rejected |
| Negative purchase value | Rejected |
| Purchase received before player Init | Held pending |
| Late player Init received | Pending purchase recovered |

After these tests, the large-test Kafka topics contained 1,002 clean events and three invalid events.

The clean total includes the original 1,000 valid events, the recovered purchase and the late Init event.

## 6. Notes

These are results from local assessment runs, not production performance benchmarks.

The generated Parquet files, Spark checkpoints and local SQLite databases remain excluded from the repository. The main project README explains how the pipeline was implemented and tested.