from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder \
    .appName("BroadcastJoinDemo") \
    .getOrCreate()

orders_data = [
    (101, 1, 1200),
    (102, 2, 800),
    (103, 1, 1500),
    (104, 3, 2000),
    (105, 2, 900)
]

customers_data = [
    (1, "Ravi"),
    (2, "Kumar"),
    (3, "Suresh")
]

orders = spark.createDataFrame(
    orders_data,
    ["order_id", "customer_id", "amount"]
)

customers = spark.createDataFrame(
    customers_data,
    ["customer_id", "customer_name"]
)

print("\n===== ORDERS =====")
orders.show()

print("\n===== CUSTOMERS =====")
customers.show()

print("\n===== SHUFFLE JOIN =====")
shuffle_join = orders.join(
    customers,
    on="customer_id",
    how="inner"
)

shuffle_join.explain(True)
shuffle_join.show()

print("\n===== BROADCAST JOIN =====")
broadcast_join = orders.join(
    broadcast(customers),
    on="customer_id",
    how="inner"
)

broadcast_join.explain(True)
broadcast_join.show()

print("\n===== VALIDATION =====")
print("Shuffle Join Rows:", shuffle_join.count())
print("Broadcast Join Rows:", broadcast_join.count())

spark.stop()
