
from pathlib import Path

from pyspark.sql import SparkSession


# Paths inside the Spark Docker container.
INPUT_PATH = "/opt/spark/sample_output/events.jsonl"
SQL_PATH = "/opt/spark/sql/03_daily_active_users.sql"
OUTPUT_PATH = "/opt/spark/sample_output/daily_active_users"


def main():
    spark = (
        SparkSession.builder
        .appName("Miniclip-Daily-Active-Users")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Load the generated events for batch processing.
        events_df = spark.read.json(INPUT_PATH)

        # Register the source DataFrame for Spark SQL.
        events_df.createOrReplaceTempView("raw_events")

        # Read the aggregation logic from its separate SQL file.
        sql_query = Path(SQL_PATH).read_text(
            encoding="utf-8"
        ).strip().rstrip(";")

        # Execute the daily active users aggregation.
        daily_users_df = spark.sql(sql_query)

        print("\n=== DAILY ACTIVE USERS ===")
        daily_users_df.show(truncate=False)

        # Save results in Parquet format.
        daily_users_df.write.mode("overwrite").parquet(
            OUTPUT_PATH
        )

        print(f"\nResults saved to: {OUTPUT_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
