from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("EcommerceDeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

source = "hdfs://h3n1:9000/ecommerce/processed/order_summary_run1"
target = "hdfs://h3n1:9000/ecommerce/delta/order_summary"

# Read existing processed CSV output
df = spark.read.option("header", True).csv(source)

# Create a new Delta table without overwriting existing data
df.write.format("delta").mode("errorifexists").save(target)

print("===== DELTA TABLE CREATED =====")
spark.read.format("delta").load(target).show(truncate=False)

spark.stop()
