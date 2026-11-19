from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, when, concat_ws, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("EcommerceKafkaDeltaDataQualityV2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
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
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

validated_df = parsed_df.withColumn(
    "dq_reason",
    concat_ws(
        " | ",
        when(col("order_id").isNull(), lit("MISSING_ORDER_ID")),
        when(col("customer_id").isNull(), lit("MISSING_CUSTOMER_ID")),
        when(col("product_id").isNull(), lit("MISSING_PRODUCT_ID")),
        when(col("quantity").isNull() | (col("quantity") <= 0), lit("INVALID_QUANTITY")),
        when(col("amount").isNull() | (col("amount") <= 0), lit("INVALID_AMOUNT")),
        when(
            col("status").isNull() | (~col("status").isin(
                "PLACED", "SHIPPED", "DELIVERED", "CANCELLED"
            )),
            lit("INVALID_STATUS")
        )
    )
).withColumn("processed_at", current_timestamp())


def process_batch(batch_df, batch_id):
    total_count = batch_df.count()

    if total_count == 0:
        print(f"Batch {batch_id}: Empty batch")
        return

    print(f"\n========== DATA QUALITY BATCH {batch_id} ==========")

    valid_raw = batch_df.filter(col("dq_reason") == "")
    invalid_records = batch_df.filter(col("dq_reason") != "")

    # Deduplicate VALID records only. Preserve every invalid record.
    valid_records = valid_raw.dropDuplicates(["order_id"])

    valid_count = valid_records.count()
    invalid_count = invalid_records.count()

    print("Total records:", total_count)
    print("Valid records (deduplicated):", valid_count)
    print("Invalid records (preserved):", invalid_count)

    if valid_count > 0:
        valid_records.drop("dq_reason") \
            .write.format("delta") \
            .mode("append") \
            .save("hdfs://h3n1:9000/ecommerce/data_quality/stream_demo_v2/valid")

    if invalid_count > 0:
        invalid_records \
            .write.format("delta") \
            .mode("append") \
            .save("hdfs://h3n1:9000/ecommerce/data_quality/stream_demo_v2/quarantine")

    print(f"Batch {batch_id} processed successfully.")


query = validated_df.writeStream \
    .foreachBatch(process_batch) \
    .option(
        "checkpointLocation",
        "hdfs://h3n1:9000/ecommerce/data_quality/stream_demo_v2/checkpoints"
    ) \
    .trigger(processingTime="30 seconds") \
    .start()

print("Kafka -> Spark -> Data Quality Streaming V2 started.")
print("Valid Output: /ecommerce/data_quality/stream_demo_v2/valid")
print("Quarantine: /ecommerce/data_quality/stream_demo_v2/quarantine")
print("Checkpoint: /ecommerce/data_quality/stream_demo_v2/checkpoints")

query.awaitTermination()
