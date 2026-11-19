from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkMemoryDemo") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

sc = spark.sparkContext
conf = sc.getConf()

print("\n===== SPARK MEMORY CONFIGURATION =====")

settings = [
    "spark.executor.memory",
    "spark.driver.memory",
    "spark.memory.fraction",
    "spark.memory.storageFraction",
    "spark.memory.offHeap.enabled"
]

for setting in settings:
    print(setting, "=", conf.get(setting, "Default"))

print("\n===== DATA PROCESSING =====")

df = spark.range(0, 1000000)

df.cache()

print("Total records:", df.count())
print("Cached records:", df.count())

print("\n===== STORAGE LEVEL =====")
print(df.storageLevel)

df.unpersist()
spark.stop()
