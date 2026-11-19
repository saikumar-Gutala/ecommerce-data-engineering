from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)

spark = SparkSession.builder \
    .appName("EcommerceKafkaDeltaStreaming") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    ) \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_orders") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df.select(
    from_json(
        col("value").cast("string"), schema
    ).alias("data")
).select("data.*")

valid_df = parsed_df.filter(
    col("order_id").isNotNull()
).withColumn(
    "processed_at", current_timestamp()
)

query = valid_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option(
        "checkpointLocation",
        "hdfs://h3n1:9000/ecommerce/streaming/checkpoints/delta_orders"
    ) \
    .trigger(processingTime="30 seconds") \
    .start(
        "hdfs://h3n1:9000/ecommerce/streaming/delta/orders"
    )

print("Kafka to Spark Delta Streaming started.")
print("Delta Output: /ecommerce/streaming/delta/orders")
print("Checkpoint: /ecommerce/streaming/checkpoints/delta_orders")

query.awaitTermination()
