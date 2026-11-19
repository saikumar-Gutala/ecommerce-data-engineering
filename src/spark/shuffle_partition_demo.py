from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ShufflePartitionDemo") \
    .getOrCreate()

path = "hdfs://h3n1:9000/ecommerce/raw/order_items.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv(path)

print("\n===== ORIGINAL DATA =====")
print("Original partitions:", df.rdd.getNumPartitions())

print("\n===== REPARTITION(4) =====")
repartitioned = df.repartition(4)
print("Partitions:", repartitioned.rdd.getNumPartitions())
repartitioned.explain(True)

print("\n===== COALESCE(1) =====")
coalesced = df.coalesce(1)
print("Partitions:", coalesced.rdd.getNumPartitions())
coalesced.explain(True)

print("\n===== VERIFY DATA =====")
print("Original rows:", df.count())
print("Repartitioned rows:", repartitioned.count())
print("Coalesced rows:", coalesced.count())

spark.stop()
