from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("EcommerceDeltaHistory") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

target = "hdfs://h3n1:9000/ecommerce/delta/order_summary"

delta_table = DeltaTable.forPath(spark, target)

print("===== DELTA TABLE HISTORY =====")
delta_table.history().select(
    "version", "timestamp", "operation"
).show(truncate=False)

print("===== ORIGINAL VERSION 0 =====")
original = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load(target)

original.show(truncate=False)

spark.stop()
