from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, expr, concat, length, sum
)

spark = SparkSession.builder \
    .appName("AQESkewJoinDemo") \
    .config("spark.sql.shuffle.partitions", "5") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1024") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1048576") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

print("\n===== SPARK AQE CONFIGURATION =====")
print("Spark Version:", spark.version)
print("AQE Enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
print("Skew Join Enabled:", spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))
print("Shuffle Partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# Create 200,000 orders.
# Customer 1 receives 180,000 orders (hot key).
orders = spark.range(0, 200000, 1, 8) \
    .withColumn(
        "customer_id",
        when(col("id") < 180000, lit(1))
        .otherwise((col("id") % 10) + 2)
    ) \
    .withColumn("payload", expr("repeat('x', 100)"))

# Create customer dimension table.
customers = spark.range(1, 12) \
    .select(
        col("id").alias("customer_id"),
        concat(lit("Customer_"), col("id")).alias("customer_name")
    )

print("\n===== CUSTOMER SKEW DISTRIBUTION =====")
orders.groupBy("customer_id") \
    .count() \
    .orderBy("customer_id") \
    .show()

print("\n===== AQE SKEW JOIN EXECUTION =====")
result = orders.join(customers, on="customer_id", how="inner")

# Force execution while using the payload column.
result.agg(
    sum(length(col("payload"))).alias("total_payload_chars")
).show()

print("\n===== DIRECT JOIN ACTION =====")
join_rows = result.rdd.mapPartitions(lambda rows: [sum(1 for row in rows)]).sum()
print("Join rows:", join_rows)

print("\n===== FINAL EXECUTED ADAPTIVE PLAN =====")
print(result._jdf.queryExecution().executedPlan().toString())

spark.stop()
