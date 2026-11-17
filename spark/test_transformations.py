
import json
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, "/opt/spark/work-dir/data_quality")
from transformations import apply_transformations


spark = (
    SparkSession.builder
    .appName("TestTransformations")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    with open(
        "/opt/spark/work-dir/data_quality/transformation_rules.json",
        encoding="utf-8"
    ) as file:
        rules = json.load(file)

    events = [
        ("init", "android", "DE"),
        ("init", "ios", "India"),
        ("init", "web", "AU"),
        ("match", None, None)
    ]

    df = spark.createDataFrame(
        events,
        ["event-type", "platform", "country"]
    )

    result = apply_transformations(df, rules)

    result.show(truncate=False)

    actual = {
        row["event-type"] + str(index): (
            row["platform"],
            row["country"]
        )
        for index, row in enumerate(result.collect())
    }

    assert actual == {
        "init0": ("ANDROID", "Germany"),
        "init1": ("IOS", "India"),
        "init2": ("WEB", "Australia"),
        "match3": (None, None)
    }, "Transformation results do not match expected values"

    print("PASS: All transformation tests passed")

finally:
    spark.stop()
