
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def main():
    if len(sys.argv) != 2:
        raise ValueError("Usage: run_sql.py <sql_filename>")

    sql_filename = Path(sys.argv[1]).name
    sql_path = Path("/opt/spark/sql") / sql_filename

    spark = (
        SparkSession.builder
        .appName("Miniclip-Spark-SQL")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Load the generated events.
        events_df = spark.read.json(
            "/opt/spark/sample_output/events.jsonl"
        )

        # Make the events accessible to Spark SQL.
        events_df.createOrReplaceTempView("raw_events")

        # Read SQL statements from a separate file.
        sql_text = sql_path.read_text(encoding="utf-8")

        # Remove comment-only lines and execute each query.
        sql_lines = [
            line for line in sql_text.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]

        for index, statement in enumerate(
            "\n".join(sql_lines).split(";"), start=1
        ):
            statement = statement.strip()

            if not statement:
                continue

            print(f"\n=== SQL QUERY {index} ===")
            spark.sql(statement).show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
