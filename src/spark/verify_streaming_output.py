from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyEcommerceStreamingOutput") \
    .getOrCreate()

path = "hdfs://h3n1:9000/ecommerce/streaming/orders"

df = spark.read.parquet(path)

print("===== STREAMING ORDER RECORDS =====")
df.show(50, truncate=False)

print("===== TOTAL RECORD COUNT =====")
print(df.count())

print("===== ORDER STATUS SUMMARY =====")
df.groupBy("status").count().show()

spark.stop()
