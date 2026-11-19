from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataSkewDemoV2") \
    .getOrCreate()

sc = spark.sparkContext

def generate_records(partition_id, iterator):
    sizes = [100, 100, 10000, 100]
    for i in range(sizes[partition_id]):
        yield (partition_id, i)

base = sc.parallelize(range(4), 4)

skewed_rdd = base.mapPartitionsWithIndex(generate_records)

print("\n===== ACTUAL PARTITION DISTRIBUTION =====")

distribution = skewed_rdd.mapPartitionsWithIndex(
    lambda index, iterator: [(index, sum(1 for _ in iterator))]
).collect()

for partition_id, count in distribution:
    print("Partition:", partition_id, "| Records:", count)

print("\n===== TOTAL RECORDS =====")
print("Total:", skewed_rdd.count())

spark.stop()
