from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum

spark = SparkSession.builder \
    .appName("DataQualityMetrics") \
    .getOrCreate()

data = [
    ("ORD6001", "CUST001", "PROD001", 2, 1500.0, "PLACED"),
    ("ORD6002", "CUST002", "PROD002", 1, 500.0, "SHIPPED"),
    ("ORD6003", None, "PROD003", 3, 900.0, "PLACED"),
    ("ORD6004", "CUST004", "PROD004", -2, 1200.0, "PLACED"),
    ("ORD6005", "CUST005", "PROD005", 2, -100.0, "PLACED"),
    ("ORD6006", "CUST006", "PROD006", 1, 300.0, "UNKNOWN"),
    (None, "CUST007", "PROD007", 2, 400.0, "PLACED"),
    ("ORD6008", "CUST008", None, 1, 250.0, "PLACED")
]

columns = ["order_id", "customer_id", "product_id",
           "quantity", "amount", "status"]

df = spark.createDataFrame(data, columns)

# Individual rule checks
checked_df = df.withColumn(
    "missing_order_id",
    when(col("order_id").isNull() | (col("order_id") == ""), 1).otherwise(0)
).withColumn(
    "missing_customer_id",
    when(col("customer_id").isNull() | (col("customer_id") == ""), 1).otherwise(0)
).withColumn(
    "missing_product_id",
    when(col("product_id").isNull() | (col("product_id") == ""), 1).otherwise(0)
).withColumn(
    "invalid_quantity",
    when(col("quantity") <= 0, 1).otherwise(0)
).withColumn(
    "invalid_amount",
    when(col("amount") <= 0, 1).otherwise(0)
).withColumn(
    "invalid_status",
    when(~col("status").isin("PLACED", "SHIPPED", "DELIVERED", "CANCELLED"), 1).otherwise(0)
)

# Overall validity
checked_df = checked_df.withColumn(
    "is_valid",
    when(
        (col("missing_order_id") == 0) &
        (col("missing_customer_id") == 0) &
        (col("missing_product_id") == 0) &
        (col("invalid_quantity") == 0) &
        (col("invalid_amount") == 0) &
        (col("invalid_status") == 0),
        1
    ).otherwise(0)
)

metrics = checked_df.agg(
    spark_sum("is_valid").alias("valid_records"),
    (spark_sum(when(col("is_valid") == 0, 1).otherwise(0))).alias("invalid_records"),
    spark_sum("missing_order_id").alias("missing_order_id_count"),
    spark_sum("missing_customer_id").alias("missing_customer_id_count"),
    spark_sum("missing_product_id").alias("missing_product_id_count"),
    spark_sum("invalid_quantity").alias("invalid_quantity_count"),
    spark_sum("invalid_amount").alias("invalid_amount_count"),
    spark_sum("invalid_status").alias("invalid_status_count")
).collect()[0]

total = df.count()
valid = metrics["valid_records"]
invalid = metrics["invalid_records"]

validity_rate = (valid / total * 100) if total else 0
invalid_rate = (invalid / total * 100) if total else 0

print("\n========== DATA QUALITY REPORT ==========")
print(f"Total Records              : {total}")
print(f"Valid Records             : {valid}")
print(f"Invalid Records           : {invalid}")
print(f"Validity Rate             : {validity_rate:.2f}%")
print(f"Invalid Rate              : {invalid_rate:.2f}%")

print("\n========== RULE-WISE FAILURES ==========")
print(f"Missing Order ID          : {metrics['missing_order_id_count']}")
print(f"Missing Customer ID      : {metrics['missing_customer_id_count']}")
print(f"Missing Product ID      : {metrics['missing_product_id_count']}")
print(f"Invalid Quantity        : {metrics['invalid_quantity_count']}")
print(f"Invalid Amount         : {metrics['invalid_amount_count']}")
print(f"Invalid Status         : {metrics['invalid_status_count']}")

print("\n========== RECORD DETAILS ==========")
checked_df.show(truncate=False)

spark.stop()
