from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("EcommerceGeneratedDeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

source = "hdfs://h3n1:9000/ecommerce/generated/processed/order_summary"
target = "hdfs://h3n1:9000/ecommerce/generated/delta/order_summary"

df = spark.read.option("header", True).option("inferSchema", True).csv(source)

df.write.format("delta").mode("errorifexists").save(target)

delta_df = spark.read.format("delta").load(target)

print("===== GENERATED DELTA TABLE =====")
delta_df.show(10, truncate=False)

print("Total Delta records:", delta_df.count())
print("Delta table location:", target)

spark.stop()
