from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder.appName("EcommerceBaselineExplain").getOrCreate()

base = "hdfs://h3n1:9000/ecommerce/raw/"

customers = spark.read.option("header", True).option("inferSchema", True).csv(base + "customers.csv")
products = spark.read.option("header", True).option("inferSchema", True).csv(base + "products.csv")
orders = spark.read.option("header", True).option("inferSchema", True).csv(base + "orders.csv")
items = spark.read.option("header", True).option("inferSchema", True).csv(base + "order_items.csv")

o, c, i, p = orders.alias("o"), customers.alias("c"), items.alias("i"), products.alias("p")

result = o.join(c, col("o.customer_id") == col("c.customer_id")) \
    .join(i, col("o.order_id") == col("i.order_id")) \
    .join(p, col("i.product_id") == col("p.product_id")) \
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.order_date"),
        col("o.status"),
        col("i.quantity"),
        col("i.unit_price").alias("item_unit_price")
    )

summary = result.filter(col("status") == "COMPLETED") \
    .withColumn("item_revenue", col("quantity") * col("item_unit_price")) \
    .groupBy("order_id", "customer_id", "customer_name", "city", "order_date") \
    .agg(spark_sum("item_revenue").alias("total_revenue"))

print("===== BASELINE PHYSICAL EXECUTION PLAN =====")
summary.explain(True)

spark.stop()
