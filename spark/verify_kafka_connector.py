
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VerifyKafkaConnector")
    .master("local[2]")
    .getOrCreate()
)

try:
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "raw-events")
        .option("startingOffsets", "earliest")
        .load()
    )

    print("PASS: Spark Kafka connector loaded successfully")
    kafka_df.printSchema()

finally:
    spark.stop()
