def detect_log_errors(log_text):

    errors = []

    log_lower = log_text.lower()

    if "schema" in log_lower or "type" in log_lower:
        errors.append({
            "error_type": "SCHEMA_DRIFT",
            "severity": "HIGH"
        })

    if "null" in log_lower or "missing" in log_lower:
        errors.append({
            "error_type": "DATA_QUALITY",
            "severity": "MEDIUM"
        })

    if "connection" in log_lower or "timeout" in log_lower:
        errors.append({
            "error_type": "CONNECTION_FAILURE",
            "severity": "HIGH"
        })

    if "failed" in log_lower or "exception" in log_lower:
        errors.append({
            "error_type": "PIPELINE_FAILURE",
            "severity": "HIGH"
        })

    return errors