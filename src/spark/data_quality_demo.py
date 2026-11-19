from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws

spark = SparkSession.builder \
    .appName("EcommerceDataQualityDemo") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

sample_data = [
    ("ORD5001", "CUST001", "PROD001", 2, 1500.0, "PLACED"),
    ("ORD5002", "CUST002", "PROD002", 1, 500.0, "SHIPPED"),
    ("ORD5003", None, "PROD003", 3, 900.0, "PLACED"),
    ("ORD5004", "CUST004", "PROD004", -2, 1200.0, "PLACED"),
    ("ORD5005", "CUST005", "PROD005", 2, -100.0, "PLACED"),
    ("ORD5006", "CUST006", "PROD006", 1, 300.0, "UNKNOWN"),
    ("", "CUST007", "PROD007", 2, 400.0, "PLACED"),
    ("ORD5008", "CUST008", None, 1, 250.0, "PLACED")
]

columns = [
    "order_id", "customer_id", "product_id",
    "quantity", "amount", "status"
]

df = spark.createDataFrame(sample_data, columns)

checked_df = df.withColumn(
    "dq_reason",
    concat_ws(
        " | ",
        when(col("order_id").isNull() | (col("order_id") == ""),
             "Missing order_id"),
        when(col("customer_id").isNull() | (col("customer_id") == ""),
             "Missing customer_id"),
        when(col("product_id").isNull() | (col("product_id") == ""),
             "Missing product_id"),
        when(col("quantity").isNull() | (col("quantity") <= 0),
             "Invalid quantity"),
        when(col("amount").isNull() | (col("amount") <= 0),
             "Invalid amount"),
        when(~col("status").isin(
            "PLACED", "SHIPPED", "DELIVERED", "CANCELLED"
        ) | col("status").isNull(),
             "Invalid status")
    )
)

valid_df = checked_df.filter(col("dq_reason") == "")
invalid_df = checked_df.filter(col("dq_reason") != "")

print("========== VALID RECORDS ==========")
valid_df.drop("dq_reason").show(truncate=False)

print("========== INVALID RECORDS ==========")
invalid_df.show(truncate=False)

print("========== DATA QUALITY SUMMARY ==========")
print("Total Records:", df.count())
print("Valid Records:", valid_df.count())
print("Invalid Records:", invalid_df.count())

spark.stop()
