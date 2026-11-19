from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("SparkUIDemo") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

print("\nSpark UI URL:", spark.sparkContext.uiWebUrl)

df = spark.range(0, 1000000, 1, 8)

result = df.withColumn(
    "category",
    col("id") % 10
).groupBy(
    "category"
).count()

result.show()

print("\n===== EXECUTION COMPLETED =====")

input("Press ENTER to stop Spark application...")

spark.stop()
