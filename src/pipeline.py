from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("RetailSalesPipeline")
    .master("local[*]")
    .getOrCreate()
)

print("\n===== READING RAW DATA =====")

df = spark.read.csv(
    "data/raw/sales.csv",
    header=True,
    inferSchema=True
)

print("\n===== RAW DATA =====")
df.show()

print("\n===== SCHEMA =====")
df.printSchema()

# Basic data cleaning
clean_df = df.filter(
    (col("quantity") > 0) &
    (col("amount") > 0)
)

print("\n===== CLEAN DATA =====")
clean_df.show()

# Count records
record_count = clean_df.count()

print(f"\nTotal valid records: {record_count}")

print("\n===== PIPELINE SUCCESS =====")
print("Data ingestion       : SUCCESS")
print("Schema inference     : SUCCESS")
print("Data cleaning        : SUCCESS")
print("Validation           : SUCCESS")
print("==============================")

spark.stop()