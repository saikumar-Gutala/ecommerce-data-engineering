from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder \
    .appName("PerformanceTuningDemo") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

orders = spark.range(0, 100000) \
    .withColumnRenamed("id", "order_id") \
    .withColumn("customer_id", col("order_id") % 1000) \
    .withColumn("amount", (col("order_id") % 5000) + 100)

customers = spark.range(0, 1000) \
    .withColumnRenamed("id", "customer_id") \
    .withColumn("customer_name", col("customer_id").cast("string"))

print("\n===== NORMAL QUERY =====")

normal = orders.join(customers, "customer_id") \
    .filter(col("amount") > 3000)

normal.explain()

print("\n===== OPTIMIZED QUERY =====")

optimized = orders.select(
    "order_id", "customer_id", "amount"
).filter(
    col("amount") > 3000
).join(
    broadcast(customers.select("customer_id", "customer_name")),
    "customer_id"
)

optimized.explain()

print("\n===== VALIDATION =====")
print("Normal count:", normal.count())
print("Optimized count:", optimized.count())

spark.stop()
