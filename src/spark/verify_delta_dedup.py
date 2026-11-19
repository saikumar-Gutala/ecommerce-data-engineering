from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("VerifyDeltaDedup") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load(
    "hdfs://h3n1:9000/ecommerce/streaming/delta/orders_dedup"
)

print("ORD4001 ROW COUNT:")
df.filter(col("order_id") == "ORD4001").groupBy("order_id").count().show()

print("ORD4001 RECORDS:")
df.filter(col("order_id") == "ORD4001").show(truncate=False)

spark.stop()
