from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("EcommerceGeneratedDeltaMerge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

source_path = "hdfs://h3n1:9000/ecommerce/generated/processed/order_summary"
target_path = "hdfs://h3n1:9000/ecommerce/generated/delta/order_summary"

# Read actual Spark-generated order summaries
source = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(source_path)

# Remove duplicate order IDs from the source
source = source.dropDuplicates(["order_id"])

source_count = source.count()

print("Source records:", source_count)

# Merge actual source data into Delta table
delta_table = DeltaTable.forPath(spark, target_path)

delta_table.alias("target").merge(
    source.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

result = spark.read.format("delta").load(target_path)

print("===== GENERATED DELTA MERGE COMPLETED =====")
print("Source records:", source_count)
print("Delta records after MERGE:", result.count())

result.orderBy("order_id").show(10, truncate=False)

spark.stop()
