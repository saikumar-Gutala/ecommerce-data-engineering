from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder \
    .appName("CachingPersistenceDemo") \
    .getOrCreate()

data = [
    (1, "Completed", 1200),
    (2, "Pending", 800),
    (3, "Completed", 1500),
    (4, "Cancelled", 600),
    (5, "Completed", 2000)
]

df = spark.createDataFrame(
    data,
    ["order_id", "status", "amount"]
)

print("\n===== ORIGINAL DATA =====")
df.show()

print("\n===== CACHE =====")
cached_df = df.cache()

print("Storage level before action:", cached_df.storageLevel)
print("Total orders:", cached_df.count())
print("Storage level after action:", cached_df.storageLevel)

print("\n===== REUSE CACHED DATA =====")
cached_df.groupBy("status").count().show()
cached_df.filter(cached_df.amount > 1000).show()

print("\n===== PERSIST =====")
persisted_df = df.persist(StorageLevel.MEMORY_AND_DISK)

print("Persisted rows:", persisted_df.count())
print("Persist storage level:", persisted_df.storageLevel)

print("\n===== UNPERSIST =====")
cached_df.unpersist()
persisted_df.unpersist()

print("Cache and persistence released.")

spark.stop()
