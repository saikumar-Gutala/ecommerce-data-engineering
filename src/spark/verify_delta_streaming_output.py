from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("VerifyEcommerceDeltaStreaming") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

path = "hdfs://h3n1:9000/ecommerce/streaming/delta/orders"

df = spark.read.format("delta").load(path)

print("\nTotal records in Delta streaming table:")
print(df.count())

print("\nChecking ORD3001:")
df.filter(col("order_id") == "ORD3001").show(truncate=False)

spark.stop()
