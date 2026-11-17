from pyspark.sql import SparkSession
from pyspark.sql.functions import col 

# Start a local Spark Session
spark = (
    SparkSession.builder
    .appName("Miniclip-Event-Exploration")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Load the simulated game events
events_df = spark.read.json(
    "/opt/spark/sample_output/events.jsonl"
)

# Inspect the structure of the dataset
print("\n=== EVENT SCHEMA ===")
events_df.printSchema()

# Check the number of records
print("\n=== TOTAL EVENT COUNT ===")
print(f"Total events: {events_df.count()}")

# Inspect the distribution of event types
print("\n=== EVENT TYPES ===")
events_df.groupBy("event-type").count().show(truncate = False)

# Examine the fields available in Init events
print("\n=== SAMPLE INIT EVENTS ===")
events_df.filter(
    col("event-type") == "init"
).select (
    col("event-type").alias("event_type"),
    col("time"),
    col("user-id").alias("user_id"),
    col("country"),
    col("platform")
).show(10, truncate = False)

spark.stop()