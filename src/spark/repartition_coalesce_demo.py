from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RepartitionCoalesceDemo") \
    .getOrCreate()

sc = spark.sparkContext

data = list(range(1, 9))
df = spark.createDataFrame([(x,) for x in data], ["id"])

print("\n===== ORIGINAL DATA =====")
print("Rows:", df.count())
print("Partitions:", df.rdd.getNumPartitions())

print("\n===== REPARTITION =====")
repartitioned = df.repartition(4)
print("Partitions:", repartitioned.rdd.getNumPartitions())
repartitioned.explain(True)

print("\n===== COALESCE =====")
coalesced = repartitioned.coalesce(2)
print("Partitions:", coalesced.rdd.getNumPartitions())
coalesced.explain(True)

print("\n===== DATA VALIDATION =====")
print("Original:", df.count())
print("Repartitioned:", repartitioned.count())
print("Coalesced:", coalesced.count())

spark.stop()
