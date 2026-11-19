from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws

spark = SparkSession.builder \
    .appName("DataQualityMultipleErrors") \
    .getOrCreate()

data = [
    (None, None, None, -2, -100.0, "UNKNOWN"),
    ("ORD5010", "CUST010", "PROD010", 2, 500.0, "PLACED")
]

columns = ["order_id", "customer_id", "product_id",
           "quantity", "amount", "status"]

df = spark.createDataFrame(data, columns)

df = df.withColumn(
    "dq_reason",
    concat_ws(
        " | ",
        when(col("order_id").isNull() | (col("order_id") == ""), "Missing order_id"),
        when(col("customer_id").isNull() | (col("customer_id") == ""), "Missing customer_id"),
        when(col("product_id").isNull() | (col("product_id") == ""), "Missing product_id"),
        when(col("quantity") <= 0, "Invalid quantity"),
        when(col("amount") <= 0, "Invalid amount"),
        when(~col("status").isin("PLACED", "SHIPPED", "DELIVERED", "CANCELLED"), "Invalid status")
    )
)

valid_df = df.filter(col("dq_reason") == "")
quarantine_df = df.filter(col("dq_reason") != "")

print("========== ALL RECORDS WITH VALIDATION RESULTS ==========")
df.show(truncate=False)

print("========== VALID RECORDS ==========")
valid_df.drop("dq_reason").show(truncate=False)

print("========== MULTIPLE ERRORS QUARANTINE ==========")
quarantine_df.show(truncate=False)

spark.stop()
