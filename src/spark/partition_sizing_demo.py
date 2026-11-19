from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("PartitionSizingDemo") \
    .getOrCreate()

file_path = "hdfs://h3n1:9000/ecommerce/raw/order_items.csv"

print("\n===== INPUT FILE PARTITION SIZING =====")

df = spark.read.option("header", True).csv(file_path)

print("Input partitions:", df.rdd.getNumPartitions())
print("Total records:", df.count())

print("\n===== SPARK FILE CONFIGURATION =====")
print("Max partition bytes:",
      spark.conf.get("spark.sql.files.maxPartitionBytes"))
print("Open cost bytes:",
      spark.conf.get("spark.sql.files.openCostInBytes"))

print("\n===== PHYSICAL PLAN =====")
df.explain(True)

spark.stop()
