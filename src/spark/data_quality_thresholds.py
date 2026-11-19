from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("DataQualityThresholds") \
    .getOrCreate()

data = [
    ("ORD7001", "CUST001", "PROD001", 2, 1500.0, "PLACED"),
    ("ORD7002", "CUST002", "PROD002", 1, 500.0, "SHIPPED"),
    ("ORD7003", None, "PROD003", 3, 900.0, "PLACED"),
    ("ORD7004", "CUST004", "PROD004", -2, 1200.0, "PLACED"),
    ("ORD7005", "CUST005", "PROD005", 2, -100.0, "PLACED"),
    ("ORD7006", "CUST006", "PROD006", 1, 300.0, "UNKNOWN"),
    (None, "CUST007", "PROD007", 2, 400.0, "PLACED"),
    ("ORD7008", "CUST008", None, 1, 250.0, "PLACED")
]

columns = ["order_id", "customer_id", "product_id",
           "quantity", "amount", "status"]

df = spark.createDataFrame(data, columns)

valid_df = df.filter(
    col("order_id").isNotNull() &
    (col("order_id") != "") &
    col("customer_id").isNotNull() &
    (col("customer_id") != "") &
    col("product_id").isNotNull() &
    (col("product_id") != "") &
    (col("quantity") > 0) &
    (col("amount") > 0) &
    col("status").isin("PLACED", "SHIPPED", "DELIVERED", "CANCELLED")
)

total = df.count()
valid = valid_df.count()
invalid = total - valid

validity_rate = (valid / total * 100) if total > 0 else 0

if validity_rate >= 99:
    quality_status = "HEALTHY"
elif validity_rate >= 95:
    quality_status = "WARNING"
else:
    quality_status = "CRITICAL"

print("\n========== DATA QUALITY ALERT REPORT ==========")
print(f"Total Records     : {total}")
print(f"Valid Records    : {valid}")
print(f"Invalid Records  : {invalid}")
print(f"Validity Rate   : {validity_rate:.2f}%")
print(f"Quality Status  : {quality_status}")

if quality_status == "HEALTHY":
    print("ALERT: No action required. Data quality is healthy.")
elif quality_status == "WARNING":
    print("ALERT: Data quality is declining. Investigate failures.")
else:
    print("CRITICAL ALERT: Data quality is below acceptable threshold!")

spark.stop()
