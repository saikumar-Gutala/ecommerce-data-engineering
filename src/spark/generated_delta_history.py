from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("EcommerceGeneratedDeltaHistory") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

target = "hdfs://h3n1:9000/ecommerce/generated/delta/order_summary"

delta_table = DeltaTable.forPath(spark, target)

print("===== DELTA HISTORY =====")
delta_table.history().select(
    "version", "timestamp", "operation"
).show(truncate=False)

print("===== PRE-MERGE DATA: VERSION 0 =====")

old_data = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load(target)

old_data.filter(
    col("order_id").isin("O000003", "O005001")
).show(truncate=False)

print("Version 0 total records:", old_data.count())

print("===== CURRENT DATA =====")

current_data = spark.read.format("delta").load(target)

current_data.filter(
    col("order_id").isin("O000003", "O005001")
).show(truncate=False)

print("Current total records:", current_data.count())

spark.stop()
