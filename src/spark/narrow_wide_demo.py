from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder \
    .appName("EcommerceNarrowWideDemo") \
    .getOrCreate()

base = "hdfs://h3n1:9000/ecommerce/raw/"

orders = spark.read.option("header", True).csv(base + "orders.csv")
items = spark.read.option("header", True).option("inferSchema", True).csv(base + "order_items.csv")

# NARROW TRANSFORMATIONS
narrow_df = orders \
    .filter(col("status") == "COMPLETED") \
    .select("order_id", "customer_id", "status")

print("\n===== NARROW TRANSFORMATION PLAN =====")
narrow_df.explain()

# WIDE TRANSFORMATION
wide_df = items \
    .groupBy("product_id") \
    .agg(spark_sum("quantity").alias("total_quantity"))

print("\n===== WIDE TRANSFORMATION PLAN =====")
wide_df.explain()

print("\n===== WIDE TRANSFORMATION RESULT =====")
wide_df.show()

spark.stop()
