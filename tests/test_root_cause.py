from src.root_cause.root_cause_engine import analyze_root_cause


# Simulated schema detector output

schema_issues = [

    {
        "column": "customer_id",
        "issue": "TYPE_CHANGED",
        "expected": "IntegerType()",
        "actual": "StringType()"
    }

]


# Simulated data quality output

quality_report = {

    "nulls": {
        "customer_id": {
            "null_count": 0,
            "null_percentage": 0
        }
    },

    "duplicates": {
        "total_rows": 10,
        "duplicate_count": 0
    },

    "negative_values": {
        "negative_amounts": 0,
        "negative_quantities": 0
    }

}


# Simulated log detector output

log_errors = [

    {
        "error_type": "SCHEMA_DRIFT",
        "severity": "HIGH",
        "message": "Schema validation failed"
    },

    {
        "error_type": "TYPE_MISMATCH",
        "severity": "HIGH",
        "message": "Column type changed from INTEGER to STRING"
    },

    {
        "error_type": "PIPELINE_FAILURE",
        "severity": "HIGH",
        "message": "PySpark pipeline execution failed"
    }

]


# Run root cause analysis

result = analyze_root_cause(
    schema_issues,
    quality_report,
    log_errors
)


print("\n==============================")
print("ROOT CAUSE ANALYSIS")
print("==============================")


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

print(
    "\nRecommendation:",
    result["recommendation"]
)


print("\nEvidence Scores:")

for cause, score in result["evidence_scores"].items():

    print(
        cause + ":",
        score
    )


print("\n==============================")