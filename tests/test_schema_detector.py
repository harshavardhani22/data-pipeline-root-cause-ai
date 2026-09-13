from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    DateType
)

from src.validation.schema_detector import detect_schema_drift


spark = (
    SparkSession.builder
    .appName("SchemaDriftTest")
    .master("local[*]")
    .getOrCreate()
)


# Expected schema
expected_schema = {
    "order_id": IntegerType(),
    "customer_id": IntegerType(),
    "product": StringType(),
    "category": StringType(),
    "quantity": IntegerType(),
    "amount": IntegerType(),
    "date": DateType()
}


# Read current dataset
df = spark.read.csv(
    "data/raw/sales.csv",
    header=True,
    inferSchema=True
)


# Detect drift
drift = detect_schema_drift(
    df,
    expected_schema
)


print("\n==============================")
print("SCHEMA DRIFT DETECTOR")
print("==============================")

if drift:

    print("🚨 SCHEMA DRIFT DETECTED")

    for issue in drift:
        print("\nColumn:", issue["column"])
        print("Issue:", issue["issue"])
        print("Expected:", issue["expected"])
        print("Actual:", issue["actual"])

else:

    print("✅ No schema drift detected")


spark.stop()