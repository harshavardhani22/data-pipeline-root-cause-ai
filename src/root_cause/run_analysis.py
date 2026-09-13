from pyspark.sql import SparkSession

from src.validation.schema_detector import detect_schema_drift
from src.detection.data_quality import generate_quality_report
from src.detection.log_detector import detect_log_errors
from src.root_cause.root_cause_engine import analyze_root_cause
from src.root_cause.impact_analysis import find_downstream_impact

from pyspark.sql.types import (
    IntegerType,
    StringType,
    DateType
)


# ==========================================
# CREATE SPARK SESSION
# ==========================================

spark = (
    SparkSession.builder
    .appName("RootCauseAI")
    .master("local[*]")
    .getOrCreate()
)


print("\n================================")
print("DATA PIPELINE ROOT-CAUSE AI")
print("================================")


# ==========================================
# 1. READ RAW DATA
# ==========================================

df = spark.read.csv(
    "data/raw/sales.csv",
    header=True,
    inferSchema=True
)


print("\n[1] DATA LOADED")
print("Records:", df.count())


# ==========================================
# 2. SCHEMA DRIFT DETECTION
# ==========================================

expected_schema = {

    "order_id": IntegerType(),

    "customer_id": IntegerType(),

    "product": StringType(),

    "category": StringType(),

    "quantity": IntegerType(),

    "amount": IntegerType(),

    "date": DateType()

}


schema_issues = detect_schema_drift(
    df,
    expected_schema
)


print("\n[2] SCHEMA CHECK")


if schema_issues:

    print("🚨 Schema drift detected")

    for issue in schema_issues:

        print(
            f"  {issue['column']}: "
            f"{issue['expected']} → "
            f"{issue['actual']}"
        )

else:

    print("✅ No schema drift")


# ==========================================
# 3. DATA QUALITY DETECTION
# ==========================================

quality_report = generate_quality_report(df)


print("\n[3] DATA QUALITY CHECK")


duplicate_count = (
    quality_report["duplicates"]["duplicate_count"]
)

negative_amounts = (
    quality_report["negative_values"]["negative_amounts"]
)

negative_quantities = (
    quality_report["negative_values"]["negative_quantities"]
)


print(
    "Duplicate rows:",
    duplicate_count
)

print(
    "Negative amounts:",
    negative_amounts
)

print(
    "Negative quantities:",
    negative_quantities
)


# ==========================================
# 4. PIPELINE LOG ANALYSIS
# ==========================================

try:

    with open(
        "logs/pipeline_failure.log",
        "r"
    ) as file:

        log_text = file.read()

except FileNotFoundError:

    print(
        "\n⚠️ pipeline_failure.log not found"
    )

    log_text = ""


log_errors = detect_log_errors(
    log_text
)


print("\n[4] LOG ANALYSIS")

print(
    "Errors detected:",
    len(log_errors)
)


if log_errors:

    for error in log_errors:

        print(
            f"  → {error['error_type']} "
            f"({error['severity']})"
        )

else:

    print("  No log errors detected")


# ==========================================
# 5. ROOT CAUSE ANALYSIS
# ==========================================

result = analyze_root_cause(

    schema_issues,

    quality_report,

    log_errors

)


# ==========================================
# 6. DOWNSTREAM IMPACT ANALYSIS
# ==========================================

impact = find_downstream_impact(
    "sales_pipeline"
)


# ==========================================
# 7. FINAL ROOT CAUSE REPORT
# ==========================================

print("\n================================")
print("🚨 ROOT CAUSE ANALYSIS")
print("================================")


print(
    "\nRoot Cause:",
    result["root_cause"]
)


print(
    "Confidence:",
    str(result["confidence"]) + "%"
)


print(
    "Severity:",
    result["severity"]
)


print("\nRecommendation:")

print(
    result["recommendation"]
)


# ==========================================
# EVIDENCE SCORES
# ==========================================

print("\nEvidence Scores:")


for cause, score in result["evidence_scores"].items():

    print(
        f"  {cause}: {score}"
    )


# ==========================================
# SCHEMA EVIDENCE
# ==========================================

if schema_issues:

    print("\nSchema Evidence:")

    for issue in schema_issues:

        print(
            f"  ✓ {issue['column']}: "
            f"{issue['expected']} → "
            f"{issue['actual']}"
        )


# ==========================================
# LOG EVIDENCE
# ==========================================

if log_errors:

    print("\nLog Evidence:")

    for error in log_errors:

        print(
            f"  ✓ {error['error_type']}: "
            f"{error.get('message', 'Detected')}"
        )


# ==========================================
# 8. DOWNSTREAM IMPACT REPORT
# ==========================================

print("\n================================")
print("DOWNSTREAM IMPACT")
print("================================")


print("\nFailed Pipeline:")

print(
    "sales_pipeline"
)


print("\nAffected Pipelines:")


for pipeline in impact["affected_pipelines"]:

    print(
        "  →",
        pipeline
    )


print("\nAffected Dashboards:")


for dashboard in impact["affected_dashboards"]:

    print(
        "  →",
        dashboard
    )


print(
    "\nPipeline Count:",
    len(impact["affected_pipelines"])
)


print(
    "Dashboard Count:",
    len(impact["affected_dashboards"])
)


# ==========================================
# 9. IMPACT LEVEL
# ==========================================

pipeline_count = len(
    impact["affected_pipelines"]
)

dashboard_count = len(
    impact["affected_dashboards"]
)


if pipeline_count >= 2 or dashboard_count >= 2:

    impact_level = "HIGH"

elif pipeline_count == 1 or dashboard_count == 1:

    impact_level = "MEDIUM"

else:

    impact_level = "LOW"


print(
    "Impact Level:",
    impact_level
)


# ==========================================
# 10. COMPLETE
# ==========================================

print("\n================================")
print("✅ ANALYSIS COMPLETE")
print("================================")


# Stop Spark

spark.stop()