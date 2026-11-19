from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkConfigInspection").getOrCreate()
sc = spark.sparkContext

configs = [
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.files.maxPartitionBytes",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled"
]

print("\n===== SPARK CONFIGURATION INSPECTION =====")

for key in configs:
    try:
        print(f"{key} = {spark.conf.get(key)}")
    except Exception as e:
        print(f"{key} = NOT AVAILABLE ({e})")

print("\n===== SPARK CORE =====")
print("Spark Version:", spark.version)
print("Default Parallelism:", sc.defaultParallelism)
print("Master:", sc.master)

spark.stop()
