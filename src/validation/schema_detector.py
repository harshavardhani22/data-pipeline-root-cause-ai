from pyspark.sql.types import IntegerType, StringType, DoubleType, DateType


def detect_schema_drift(df, expected_schema):

    actual_schema = {
        field.name: field.dataType
        for field in df.schema.fields
    }

    drift_detected = []

    for column, expected_type in expected_schema.items():

        if column not in actual_schema:
            drift_detected.append({
                "column": column,
                "issue": "COLUMN_MISSING",
                "expected": expected_type,
                "actual": "MISSING"
            })

        else:
            actual_type = actual_schema[column]

            if actual_type != expected_type:
                drift_detected.append({
                    "column": column,
                    "issue": "TYPE_CHANGED",
                    "expected": expected_type,
                    "actual": actual_type
                })

    return drift_detected