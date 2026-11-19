from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TaskExecutionDemo") \
    .getOrCreate()

sc = spark.sparkContext

data = list(range(1, 9))

rdd = sc.parallelize(data, 8)

print("\n===== PARTITION COUNT =====")
print("Partitions:", rdd.getNumPartitions())

print("\n===== PARTITION CONTENTS =====")
def inspect_partition(index, iterator):
    values = list(iterator)
    yield (index, values)

results = rdd.mapPartitionsWithIndex(
    inspect_partition
).collect()

for partition_id, values in results:
    print("Partition:", partition_id, "| Values:", values)

print("\n===== TOTAL RECORDS =====")
print("Count:", rdd.count())

spark.stop()
