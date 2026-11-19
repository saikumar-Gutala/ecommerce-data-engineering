from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, lit, monotonically_increasing_id, expr
)

spark = SparkSession.builder \
    .appName("DataSkewSaltingDemo") \
    .config("spark.sql.shuffle.partitions", "5") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

# Create skewed orders
orders_data = []

for i in range(10000):
    orders_data.append((i + 1, 1, 1000))

for customer_id in range(2, 6):
    for i in range(100):
        orders_data.append((
            20000 + customer_id * 100 + i,
            customer_id,
            500
        ))

orders = spark.createDataFrame(
    orders_data,
    ["order_id", "customer_id", "amount"]
)

customers = spark.createDataFrame(
    [(1, "Ravi"), (2, "Kumar"), (3, "Suresh"),
     (4, "Ajay"), (5, "Vijay")],
    ["customer_id", "customer_name"]
)

# Assign salt values to hot-key orders
salted_orders = orders.withColumn(
    "salt",
    when(
        col("customer_id") == 1,
        expr("pmod(monotonically_increasing_id(), 5)")
    ).otherwise(lit(0))
)

# Generate five matching copies of hot customer
salt_values = spark.createDataFrame(
    [(0,), (1,), (2,), (3,), (4,)],
    ["salt"]
)

hot_customers = customers.filter(
    col("customer_id") == 1
).crossJoin(salt_values)

# Keep normal customers with salt 0
normal_customers = customers.filter(
    col("customer_id") != 1
).withColumn("salt", lit(0))

# Both DataFrames now have the same three columns
salted_customers = hot_customers.unionByName(normal_customers)

print("\n===== SALTED ORDER DISTRIBUTION =====")
salted_orders.groupBy("customer_id", "salt") \
    .agg(count("*").alias("records")) \
    .orderBy("customer_id", "salt") \
    .show(20)

print("\n===== SALTED CUSTOMER DATA =====")
salted_customers.orderBy("customer_id", "salt").show(10)

print("\n===== SALTED JOIN PHYSICAL PLAN =====")
result = salted_orders.join(
    salted_customers,
    on=["customer_id", "salt"],
    how="inner"
)

result.explain(True)

print("\n===== FINAL VALIDATION =====")
print("Original orders:", orders.count())
print("Salted join rows:", result.count())

spark.stop()
