
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

OUTPUT_PATH = "/opt/spark/sample_output/daily_active_users"

spark = (
    SparkSession.builder
    .appName("Miniclip-Verify-Daily-Active-Users")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # Read the saved Parquet output.
    result_df = spark.read.parquet(OUTPUT_PATH)

    print("\n=== SAVED PARQUET DATA ===")
    result_df.orderBy(
        "event_date", "country", "platform"
    ).show(truncate=False)

    # Validate the expected sample results.
    row_count = result_df.count()

    total_users = result_df.agg(
        spark_sum("daily_active_users").alias("total")
    ).first()["total"]

    dates = [
        row["event_date"]
        for row in result_df.select("event_date").distinct().collect()
    ]

    assert row_count == 6, f"Expected 6 rows, got {row_count}"
    assert total_users == 10, f"Expected 10 users, got {total_users}"
    assert len(dates) == 1 and str(dates[0]) == "2026-11-15", (
        f"Unexpected dates: {dates}"
    )

    print("\n=== VALIDATION RESULTS ===")
    print(f"Output rows: {row_count}")
    print(f"Sum of daily active users: {total_users}")
    print(f"Event dates: {dates}")
    print("PASS: Saved Parquet results match expected sample totals")

finally:
    spark.stop()
