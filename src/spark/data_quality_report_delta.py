from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

spark = SparkSession.builder \
    .appName("DataQualityReportDelta") \
    .getOrCreate()

data = [
    ("ORD8001", "CUST001", "PROD001", 2, 1500.0, "PLACED"),
    ("ORD8002", "CUST002", "PROD002", 1, 500.0, "SHIPPED"),
    ("ORD8003", None, "PROD003", 3, 900.0, "PLACED"),
    ("ORD8004", "CUST004", "PROD004", -2, 1200.0, "PLACED"),
    ("ORD8005", "CUST005", "PROD005", 2, -100.0, "PLACED"),
    ("ORD8006", "CUST006", "PROD006", 1, 300.0, "UNKNOWN"),
    (None, "CUST007", "PROD007", 2, 400.0, "PLACED"),
    ("ORD8008", "CUST008", None, 1, 250.0, "PLACED")
]

columns = ["order_id", "customer_id", "product_id",
           "quantity", "amount", "status"]

df = spark.createDataFrame(data, columns)

valid_df = df.filter(
    col("order_id").isNotNull() & (col("order_id") != "") &
    col("customer_id").isNotNull() & (col("customer_id") != "") &
    col("product_id").isNotNull() & (col("product_id") != "") &
    (col("quantity") > 0) &
    (col("amount") > 0) &
    col("status").isin("PLACED", "SHIPPED", "DELIVERED", "CANCELLED")
)

total = df.count()
valid = valid_df.count()
invalid = total - valid

validity_rate = (valid / total * 100) if total else 0
invalid_rate = (invalid / total * 100) if total else 0

if validity_rate >= 99:
    status = "HEALTHY"
elif validity_rate >= 95:
    status = "WARNING"
else:
    status = "CRITICAL"

run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

report_data = [
    (run_timestamp, total, valid, invalid,
     float(validity_rate), float(invalid_rate), status)
]

report_columns = [
    "run_timestamp", "total_records", "valid_records",
    "invalid_records", "validity_rate", "invalid_rate",
    "quality_status"
]

report_df = spark.createDataFrame(report_data, report_columns)

report_path = "hdfs://h3n1:9000/ecommerce/data_quality/demo/reports"

report_df.write \
    .format("delta") \
    .mode("append") \
    .save(report_path)

print("\n========== DATA QUALITY REPORT ==========")
report_df.show(truncate=False)

print("Delta report saved to:", report_path)

spark.stop()
