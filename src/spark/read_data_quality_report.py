from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadDataQualityReport") \
    .getOrCreate()

report_path = "hdfs://h3n1:9000/ecommerce/data_quality/demo/reports"

df = spark.read.format("delta").load(report_path)

print("\n========== STORED DATA QUALITY REPORT ==========")
df.show(truncate=False)

print("\n========== REPORT SCHEMA ==========")
df.printSchema()

print("\n========== TOTAL REPORT RUNS ==========")
print("Total runs:", df.count())

spark.stop()
