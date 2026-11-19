from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)

spark = SparkSession.builder \
    .appName("EcommerceKafkaStructuredStreaming") \
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
    .format("parquet") \
    .outputMode("append") \
    .option(
        "path",
        "hdfs://h3n1:9000/ecommerce/streaming/orders"
    ) \
    .option(
        "checkpointLocation",
        "hdfs://h3n1:9000/ecommerce/streaming/checkpoints/orders"
    ) \
    .trigger(processingTime="10 seconds") \
    .start()

print("E-Commerce Kafka to Spark Streaming started.")
print("Output: /ecommerce/streaming/orders")
print("Checkpoint: /ecommerce/streaming/checkpoints/orders")

query.awaitTermination()
