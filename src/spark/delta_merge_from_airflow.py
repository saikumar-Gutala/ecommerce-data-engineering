from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("EcommerceAirflowDeltaMerge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

source_path = "hdfs://h3n1:9000/ecommerce/processed/order_summary_airflow_run1"
target_path = "hdfs://h3n1:9000/ecommerce/delta/order_summary"

source = spark.read.option("header", True).csv(source_path)

delta_table = DeltaTable.forPath(spark, target_path)

delta_table.alias("target").merge(
    source.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("===== DELTA TABLE AFTER AIRFLOW MERGE =====")
spark.read.format("delta").load(target_path).show(truncate=False)

spark.stop()
