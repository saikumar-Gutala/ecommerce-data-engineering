from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder \
    .appName("EcommerceGeneratedBatchPipeline") \
    .getOrCreate()

base = "hdfs://h3n1:9000/ecommerce/generated/raw/"

customers = spark.read.option("header", True).option("inferSchema", True).csv(base + "customers.csv")
products = spark.read.option("header", True).option("inferSchema", True).csv(base + "products.csv")
orders = spark.read.option("header", True).option("inferSchema", True).csv(base + "orders.csv")
items = spark.read.option("header", True).option("inferSchema", True).csv(base + "order_items.csv")

o = orders.alias("o")
c = customers.alias("c")
i = items.alias("i")
p = products.alias("p")

result = o.join(
    c, col("o.customer_id") == col("c.customer_id")
).join(
    i, col("o.order_id") == col("i.order_id")
).join(
    p, col("i.product_id") == col("p.product_id")
).select(
    col("o.order_id"),
    col("o.customer_id"),
    col("c.customer_name"),
    col("c.city"),
    col("o.order_date"),
    col("o.status"),
    col("i.quantity"),
    col("i.unit_price").alias("item_unit_price")
)

summary = result.filter(
    col("status") == "COMPLETED"
).withColumn(
    "item_revenue",
    col("quantity") * col("item_unit_price")
).groupBy(
    "order_id",
    "customer_id",
    "customer_name",
    "city",
    "order_date"
).agg(
    spark_sum("item_revenue").alias("total_revenue")
)

print("===== GENERATED E-COMMERCE ORDER SUMMARY =====")
summary.show(10, truncate=False)

output = "hdfs://h3n1:9000/ecommerce/generated/processed/order_summary_final_validation"

summary.write.mode("overwrite").option("header", True).csv(output)

print("Output saved to:", output)
print("Total completed order summaries:", summary.count())

spark.stop()
