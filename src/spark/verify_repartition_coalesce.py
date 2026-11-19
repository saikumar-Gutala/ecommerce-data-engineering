from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyRepartitionCoalesce") \
    .getOrCreate()

df = spark.createDataFrame(
    [(i,) for i in range(1, 9)], ["id"]
)

repartitioned = df.repartition(4)
coalesced = repartitioned.coalesce(2)

print("\n===== PARTITION COUNTS =====")
print("Original:", df.rdd.getNumPartitions())
print("Repartitioned:", repartitioned.rdd.getNumPartitions())
print("Coalesced:", coalesced.rdd.getNumPartitions())

print("\n===== ROW COUNTS =====")
print("Original:", df.count())
print("Repartitioned:", repartitioned.count())
print("Coalesced:", coalesced.count())

spark.stop()
