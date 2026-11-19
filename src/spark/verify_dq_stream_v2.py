from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyDQStreamV2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

valid_path = "hdfs://h3n1:9000/ecommerce/data_quality/stream_demo_v2/valid"
quarantine_path = "hdfs://h3n1:9000/ecommerce/data_quality/stream_demo_v2/quarantine"

print("\n========== VALID RECORDS ==========")
valid = spark.read.format("delta").load(valid_path)
valid.filter(valid.order_id.startswith("DQTEST2")).show(truncate=False)

print("\n========== QUARANTINE RECORDS ==========")
invalid = spark.read.format("delta").load(quarantine_path)
invalid.show(truncate=False)

print("\n========== TOTAL COUNTS ==========")
print("Valid total:", valid.count())
print("Quarantine total:", invalid.count())

spark.stop()
