from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyCustomerRevenueDelta") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

path = "hdfs://h3n1:9000/ecommerce/delta/customer_revenue_report"

df = spark.read.format("delta").load(path)

print("===== DELTA TABLE DATA =====")
df.show()

print("Total Records:", df.count())

print("===== DELTA TABLE HISTORY =====")
from delta.tables import DeltaTable
DeltaTable.forPath(spark, path).history().select(
    "version", "operation"
).show()

spark.stop()
