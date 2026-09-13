def analyze_root_cause(schema_issues, quality_report, log_errors):

    scores = {
        "SCHEMA_DRIFT": 0,
        "DATA_QUALITY": 0,
        "PIPELINE_FAILURE": 0,
        "CONNECTION_FAILURE": 0
    }

    # -----------------------------
    # Schema evidence
    # -----------------------------

    if schema_issues:

        scores["SCHEMA_DRIFT"] += 60


    # -----------------------------
    # Data quality evidence
    # -----------------------------

    if quality_report:

        nulls = quality_report.get("nulls", {})
        duplicates = quality_report.get("duplicates", {})
        negatives = quality_report.get("negative_values", {})

        has_nulls = any(
            value["null_count"] > 0
            for value in nulls.values()
        )

        has_duplicates = (
            duplicates.get("duplicate_count", 0) > 0
        )

        has_negatives = (
            negatives.get("negative_amounts", 0) > 0
            or
            negatives.get("negative_quantities", 0) > 0
        )

        if has_nulls or has_duplicates or has_negatives:
            scores["DATA_QUALITY"] += 60


    # -----------------------------
    # Log evidence
    # -----------------------------

    for error in log_errors:

        error_type = error["error_type"]

        if error_type == "SCHEMA_DRIFT":
            scores["SCHEMA_DRIFT"] += 25

        elif error_type == "TYPE_MISMATCH":
            scores["SCHEMA_DRIFT"] += 15

        elif error_type == "PIPELINE_FAILURE":
            scores["PIPELINE_FAILURE"] += 30

        elif error_type == "CONNECTION_FAILURE":
            scores["CONNECTION_FAILURE"] += 30


    # -----------------------------
    # Find strongest root cause
    # -----------------------------

    root_cause = max(
        scores,
        key=scores.get
    )

    score = scores[root_cause]


    # -----------------------------
    # Confidence
    # -----------------------------

    confidence = min(score, 96)


    # -----------------------------
    # Recommendations
    # -----------------------------

    recommendations = {

        "SCHEMA_DRIFT":
            "Restore the expected column data type or update the downstream schema.",

        "DATA_QUALITY":
            "Investigate null, duplicate, or invalid records before processing.",

        "PIPELINE_FAILURE":
            "Inspect the failed pipeline stage and retry after resolving the underlying error.",

        "CONNECTION_FAILURE":
            "Check database/network connectivity and retry the pipeline."
    }


    return {
        "root_cause": root_cause,
        "confidence": confidence,
        "severity": "HIGH",
        "recommendation": recommendations[root_cause],
        "evidence_scores": scores
    }