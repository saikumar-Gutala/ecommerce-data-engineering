from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyInputPartitions") \
    .getOrCreate()

path = "hdfs://h3n1:9000/ecommerce/raw/order_items.csv"

df = spark.read.option("header", True).csv(path)

print("\n===== INPUT PARTITION VERIFICATION =====")
print("File records:", df.count())
print("Input partitions:", df.rdd.getNumPartitions())

print("\n===== PARTITION RECORD DISTRIBUTION =====")
distribution = df.rdd.mapPartitionsWithIndex(
    lambda index, rows: [(index, sum(1 for _ in rows))]
).collect()

for partition_id, count in distribution:
    print("Partition:", partition_id, "| Records:", count)

spark.stop()
