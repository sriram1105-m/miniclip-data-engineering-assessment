
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

INPUT_DIR = (
    "/opt/spark/sample_output/"
    "pro_purchase_counts_v3"
)


def main():
    spark = (
        SparkSession.builder
        .appName("VerifyProPurchaseCounts")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    updates = spark.read.parquet(INPUT_DIR)

    window = (
        Window
        .partitionBy("minute_start", "minute_end")
        .orderBy(F.col("batch_id").desc())
    )

    latest_counts = (
        updates
        .withColumn(
            "row_number",
            F.row_number().over(window),
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number", "batch_id")
        .orderBy("minute_start")
    )

    print("\nLATEST PURCHASE COUNTS BY MINUTE")
    latest_counts.show(truncate=False)

    total = (
        latest_counts
        .agg(F.sum("purchase_count").alias("total"))
        .first()["total"]
    )

    print(f"Total purchases: {total}")

    spark.stop()


if __name__ == "__main__":
    main()
