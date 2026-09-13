from pyspark.sql.functions import col, count, when, isnan


def check_nulls(df):
    results = {}

    total_rows = df.count()

    for column in df.columns:
        null_count = df.filter(
            col(column).isNull()
        ).count()

        null_percentage = (
            null_count / total_rows * 100
            if total_rows > 0 else 0
        )

        results[column] = {
            "null_count": null_count,
            "null_percentage": round(null_percentage, 2)
        }

    return results


def check_duplicates(df):

    total_rows = df.count()
    unique_rows = df.dropDuplicates().count()

    duplicate_count = total_rows - unique_rows

    return {
        "total_rows": total_rows,
        "duplicate_count": duplicate_count
    }


def check_negative_values(df):

    negative_amounts = df.filter(
        col("amount") < 0
    ).count()

    negative_quantities = df.filter(
        col("quantity") < 0
    ).count()

    return {
        "negative_amounts": negative_amounts,
        "negative_quantities": negative_quantities
    }


def generate_quality_report(df):

    nulls = check_nulls(df)
    duplicates = check_duplicates(df)
    negatives = check_negative_values(df)

    return {
        "nulls": nulls,
        "duplicates": duplicates,
        "negative_values": negatives
    }