# Data Pipeline Root-Cause AI

A starter project for detecting data-pipeline failures and identifying likely root causes.

## Current milestone
- PySpark retail sales pipeline
- Raw CSV input
- Basic data cleaning
- Parquet output
- Project structure ready for schema drift, data quality, logs, root-cause scoring, Airflow, dashboard, and AWS

## Run
1. Create/activate a Python virtual environment.
2. Install dependencies:
   `pip install -r requirements.txt`
3. Run:
   `python src/pipeline.py`

## Next milestone
Build `src/validation/schema_detector.py` to compare expected vs actual schemas and detect schema drift.
