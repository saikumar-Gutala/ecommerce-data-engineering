from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws

spark = SparkSession.builder \
    .appName("EcommerceDataQualityQuarantine") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
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

columns = ["order_id", "customer_id", "product_id",
           "quantity", "amount", "status"]

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
        when(col("status").isNull() | (~col("status").isin(
             "PLACED", "SHIPPED", "DELIVERED", "CANCELLED")),
             "Invalid status")
    )
)

valid_df = checked_df.filter(col("dq_reason") == "").drop("dq_reason")
quarantine_df = checked_df.filter(col("dq_reason") != "")

valid_path = "hdfs://h3n1:9000/ecommerce/data_quality/demo/valid"
quarantine_path = "hdfs://h3n1:9000/ecommerce/data_quality/demo/quarantine"

valid_df.write.format("delta").mode("overwrite").save(valid_path)
quarantine_df.write.format("delta").mode("overwrite").save(quarantine_path)

total_count = df.count()
valid_count = valid_df.count()
invalid_count = quarantine_df.count()

print("========== DATA QUALITY REPORT ==========")
print("Total records:", total_count)
print("Valid records:", valid_count)
print("Invalid records:", invalid_count)
print("Validity rate:", round(valid_count / total_count * 100, 2), "%")

print("========== VALID DELTA TABLE ==========")
spark.read.format("delta").load(valid_path).show(truncate=False)

print("========== QUARANTINE DELTA TABLE ==========")
spark.read.format("delta").load(quarantine_path).show(truncate=False)

print("Valid Delta path:", valid_path)
print("Quarantine Delta path:", quarantine_path)

spark.stop()
