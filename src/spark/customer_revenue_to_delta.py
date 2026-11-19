from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CustomerRevenueDelta") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

source = "hdfs://h3n1:9000/ecommerce/processed/customer_revenue_report"

target = "hdfs://h3n1:9000/ecommerce/delta/customer_revenue_report"

df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(source)

df.write.format("delta").mode("overwrite").save(target)

print("Customer Revenue Delta Table Created")

spark.read.format("delta").load(target).show()

spark.stop()
