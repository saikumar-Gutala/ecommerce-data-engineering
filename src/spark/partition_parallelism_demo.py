from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PartitionParallelismDemo") \
    .getOrCreate()

sc = spark.sparkContext

print("\n===== SPARK CONFIGURATION =====")
print("Spark Version:", spark.version)
print("Master:", sc.master)
print("Default Parallelism:", sc.defaultParallelism)
print("Shuffle Partitions:",
      spark.conf.get("spark.sql.shuffle.partitions"))

path = "hdfs://h3n1:9000/ecommerce/raw/order_items.csv"

df = spark.read.option("header", True) \
    .option("inferSchema", True).csv(path)

print("\n===== DATA PARTITIONS =====")
print("Input partitions:", df.rdd.getNumPartitions())

print("\n===== DATA COUNT =====")
print("Total rows:", df.count())

spark.stop()
