from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder \
    .appName("DataSkewJoinDemo") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

# Create skewed orders data
orders_data = []

# Hot key: customer_id 1 has 10000 orders
for i in range(10000):
    orders_data.append((i + 1, 1, 1000))

# Other customers have only 100 orders each
for customer_id in range(2, 6):
    for i in range(100):
        orders_data.append((20000 + customer_id * 100 + i,
                            customer_id, 500))

orders = spark.createDataFrame(
    orders_data,
    ["order_id", "customer_id", "amount"]
)

customers_data = [
    (1, "Ravi"),
    (2, "Kumar"),
    (3, "Suresh"),
    (4, "Ajay"),
    (5, "Vijay")
]

customers = spark.createDataFrame(
    customers_data,
    ["customer_id", "customer_name"]
)

print("\n===== TOTAL ORDERS =====")
print("Total:", orders.count())

print("\n===== CUSTOMER KEY FREQUENCY =====")
orders.groupBy("customer_id") \
    .agg(count("*").alias("order_count")) \
    .orderBy(col("order_count").desc()) \
    .show()

print("\n===== SKEWED JOIN =====")
skewed_join = orders.join(
    customers,
    on="customer_id",
    how="inner"
)

skewed_join.explain(True)

print("\n===== JOIN RESULT =====")
print("Joined rows:", skewed_join.count())

spark.stop()
