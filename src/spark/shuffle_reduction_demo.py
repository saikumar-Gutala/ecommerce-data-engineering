from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder \
    .appName("ShuffleReductionDemo") \
    .getOrCreate()

path = "hdfs://h3n1:9000/ecommerce/raw/order_items.csv"

df = spark.read.option("header", True) \
    .option("inferSchema", True).csv(path)

print("\n===== INPUT DATA =====")
df.show()

print("\n===== GROUP BY PRODUCT =====")
result = df.groupBy("product_id").agg(
    sum("quantity").alias("total_quantity")
)

result.explain(True)

print("\n===== FINAL RESULT =====")
result.show()

spark.stop()
