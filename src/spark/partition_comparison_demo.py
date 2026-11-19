from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PartitionComparisonDemo") \
    .getOrCreate()

path = "hdfs://h3n1:9000/ecommerce/raw/order_items.csv"

df = spark.read.option("header", True) \
    .option("inferSchema", True).csv(path)

print("\n===== PARTITION COMPARISON =====")

for n in [1, 2, 4, 8]:
    test_df = df.repartition(n)

    print(
        "Requested partitions:", n,
        "| Actual partitions:", test_df.rdd.getNumPartitions(),
        "| Rows:", test_df.count()
    )

print("\n===== ORIGINAL DATA =====")
print("Original partitions:", df.rdd.getNumPartitions())

spark.stop()
