from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CatalystTungstenDemo") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

data = [
    (1, "Laptop", 60000, "Electronics"),
    (2, "Phone", 25000, "Electronics"),
    (3, "Chair", 5000, "Furniture"),
    (4, "Table", 12000, "Furniture"),
    (5, "Tablet", 30000, "Electronics")
]

df = spark.createDataFrame(
    data,
    ["product_id", "product_name", "price", "category"]
)

result = df.select(
    "product_name", "price"
).filter(
    col("price") > 10000
)

print("\n===== CATALYST OPTIMIZATION =====")
result.explain(True)

print("\n===== RESULT =====")
result.show()

print("\n===== CODE GENERATION CONFIG =====")
print(
    "Whole Stage Codegen:",
    spark.conf.get("spark.sql.codegen.wholeStage")
)

spark.stop()
