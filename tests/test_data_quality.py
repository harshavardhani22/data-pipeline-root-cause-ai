from pyspark.sql import SparkSession

from src.detection.data_quality import generate_quality_report


spark = (
    SparkSession.builder
    .appName("DataQualityTest")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.csv(
    "data/raw/sales.csv",
    header=True,
    inferSchema=True
)

report = generate_quality_report(df)

print("\n==============================")
print("DATA QUALITY REPORT")
print("==============================")

print("\nNULL VALUES")

for column, data in report["nulls"].items():
    print(
        f"{column}: "
        f"{data['null_count']} nulls "
        f"({data['null_percentage']}%)"
    )

print("\nDUPLICATES")

print(
    "Total rows:",
    report["duplicates"]["total_rows"]
)

print(
    "Duplicate rows:",
    report["duplicates"]["duplicate_count"]
)

print("\nNEGATIVE VALUES")

print(
    "Negative amounts:",
    report["negative_values"]["negative_amounts"]
)

print(
    "Negative quantities:",
    report["negative_values"]["negative_quantities"]
)

print("\n==============================")

spark.stop()