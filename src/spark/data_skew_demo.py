from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataSkewDemo") \
    .getOrCreate()

sc = spark.sparkContext

# 4 partitions with deliberately uneven record counts
data = (
    [("P0", i) for i in range(100)] +
    [("P1", i) for i in range(100)] +
    [("P2", i) for i in range(10000)] +
    [("P3", i) for i in range(100)]
)

rdd = sc.parallelize(data, 4)

print("\n===== PARTITION DISTRIBUTION =====")

distribution = rdd.mapPartitionsWithIndex(
    lambda index, iterator: [(index, sum(1 for _ in iterator))]
).collect()

for partition_id, count in distribution:
    print("Partition:", partition_id, "| Records:", count)

print("\n===== TOTAL RECORDS =====")
print("Total:", rdd.count())

spark.stop()
