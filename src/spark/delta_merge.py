from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("EcommerceDeltaMerge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

target = "hdfs://h3n1:9000/ecommerce/delta/order_summary"

# Simulated updates and new order
updates = [
    ("O101", "C101", "Rahul", "Hyderabad", "2026-10-31", "1250.0"),
    ("O104", "C104", "Kiran", "Visakhapatnam", "2026-11-12", "650.0")
]

columns = [
    "order_id", "customer_id", "customer_name",
    "city", "order_date", "total_revenue"
]

source = spark.createDataFrame(updates, columns)

delta_table = DeltaTable.forPath(spark, target)

delta_table.alias("target").merge(
    source.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("===== AFTER MERGE =====")
spark.read.format("delta").load(target).show(truncate=False)

spark.stop()
