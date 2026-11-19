from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CheckORD3002") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

path = "hdfs://h3n1:9000/ecommerce/streaming/delta/orders"

df = spark.read.format("delta").load(path)

print("Total Delta records:", df.count())
print("ORD3002 verification:")

df.filter(col("order_id") == "ORD3002").show(truncate=False)

spark.stop()
