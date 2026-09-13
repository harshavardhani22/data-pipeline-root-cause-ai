from src.detection.log_detector import detect_log_errors


log_text = """
INFO: Starting PySpark pipeline
INFO: Reading sales data
ERROR: Schema validation failed
ERROR: customer_id expected INTEGER but received STRING
ERROR: PySpark job failed
"""


errors = detect_log_errors(log_text)


print("\n==============================")
print("LOG ERROR DETECTOR")
print("==============================")


if errors:

    print("🚨 ERRORS DETECTED")

    for error in errors:
        print("\nError Type:", error["error_type"])
        print("Severity:", error["severity"])

else:

    print("✅ No errors detected")


print("\n==============================")