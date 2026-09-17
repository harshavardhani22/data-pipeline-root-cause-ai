"""Interactive dashboard for the data-pipeline root-cause analysis project.

Run with: streamlit run dashboard.py
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, StringType

from src.detection.data_quality import generate_quality_report
from src.detection.log_detector import detect_log_errors
from src.root_cause.impact_analysis import find_downstream_impact
from src.root_cause.root_cause_engine import analyze_root_cause
from src.validation.schema_detector import detect_schema_drift


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_PATH = PROJECT_ROOT / "data" / "raw" / "sales.csv"
LOG_PATH = PROJECT_ROOT / "logs" / "pipeline_failure.log"


st.set_page_config(page_title="Pipeline Sentinel", page_icon="🔎", layout="wide")


@st.cache_resource
def get_spark() -> SparkSession:
    """Create one local Spark session for the Streamlit process."""
    return (
        SparkSession.builder.appName("PipelineSentinel")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@st.cache_data(ttl=60)
def collect_analysis() -> dict:
    """Run all existing analysis modules against the sample data and log."""
    dataframe = get_spark().read.csv(str(DATA_PATH), header=True, inferSchema=True)
    expected_schema = {
        "order_id": IntegerType(),
        "customer_id": StringType(),
        "product": StringType(),
        "category": StringType(),
        "quantity": IntegerType(),
        "amount": IntegerType(),
        "date": DateType(),
    }
    schema_issues = detect_schema_drift(dataframe, expected_schema)
    quality_report = generate_quality_report(dataframe)
    log_text = LOG_PATH.read_text(encoding="utf-8") if LOG_PATH.exists() else ""
    log_errors = detect_log_errors(log_text)
    root_cause = analyze_root_cause(schema_issues, quality_report, log_errors)
    impact = find_downstream_impact("sales_pipeline")

    return {
        "records": dataframe.count(),
        "schema_issues": schema_issues,
        "quality": quality_report,
        "log_errors": log_errors,
        "root_cause": root_cause,
        "impact": impact,
    }


def status_label(data: dict) -> str:
    return "FAILED" if data["schema_issues"] or data["log_errors"] else "HEALTHY"


with st.sidebar:
    st.title("🔎 Pipeline Sentinel")
    st.caption("Data Reliability Console")
    environment = st.selectbox("Environment", ["Production", "Staging", "Development"])
    pipeline = st.selectbox("Pipeline", ["sales_pipeline"], disabled=True)
    if st.button("Refresh analysis", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption("Results refresh automatically every 60 seconds.")


st.title("Pipeline Incident Overview")
st.caption(f"{environment} · Automated analysis for `{pipeline}`")

try:
    analysis = collect_analysis()
except Exception as error:
    st.error("The pipeline analysis could not be completed.")
    st.exception(error)
    st.stop()

quality = analysis["quality"]
root_cause = analysis["root_cause"]
impact = analysis["impact"]
duplicates = quality["duplicates"]["duplicate_count"]
negatives = quality["negative_values"]

metrics = st.columns(4)
metrics[0].metric("Pipeline status", status_label(analysis))
metrics[1].metric("Root cause", root_cause["root_cause"])
metrics[2].metric("Confidence", f"{root_cause['confidence']}%")
metrics[3].metric("Records analyzed", analysis["records"])

st.subheader("Incident diagnosis")
left, right = st.columns([3, 2])
with left:
    st.error(
        f"**Likely cause: {root_cause['root_cause']}** · "
        f"Severity: {root_cause['severity']} · Confidence: {root_cause['confidence']}%"
    )
    st.write(root_cause["recommendation"])
    schema_tab, quality_tab, logs_tab = st.tabs(["Schema", "Data quality", "Logs"])
    with schema_tab:
        if analysis["schema_issues"]:
            st.dataframe(analysis["schema_issues"], use_container_width=True, hide_index=True)
        else:
            st.success("Schema validation passed.")
    with quality_tab:
        q1, q2, q3 = st.columns(3)
        q1.metric("Duplicate rows", duplicates)
        q2.metric("Negative amounts", negatives["negative_amounts"])
        q3.metric("Negative quantities", negatives["negative_quantities"])
        null_rows = [
            {"column": column, **details}
            for column, details in quality["nulls"].items()
            if details["null_count"]
        ]
        if null_rows:
            st.dataframe(null_rows, use_container_width=True, hide_index=True)
        else:
            st.success("No null values detected.")
    with logs_tab:
        if analysis["log_errors"]:
            st.dataframe(analysis["log_errors"], use_container_width=True, hide_index=True)
        else:
            st.success("No relevant pipeline errors detected.")

with right:
    st.markdown("##### Evidence scoring")
    for cause, score in root_cause["evidence_scores"].items():
        st.write(f"**{cause.replace('_', ' ').title()}** — {score}")
        st.progress(min(score, 100) / 100)

st.subheader("Downstream impact")
i1, i2, i3 = st.columns(3)
i1.metric("Affected pipelines", len(impact["affected_pipelines"]))
i2.metric("Affected dashboards", len(impact["affected_dashboards"]))
impact_level = "HIGH" if len(impact["affected_pipelines"]) >= 2 else "MEDIUM" if impact["affected_pipelines"] else "LOW"
i3.metric("Impact level", impact_level)

impact_left, impact_right = st.columns(2)
with impact_left:
    st.markdown("##### Affected pipelines")
    st.write(impact["affected_pipelines"] or ["None"])
with impact_right:
    st.markdown("##### Affected dashboards")
    st.write(impact["affected_dashboards"] or ["None"])

st.caption(f"Last analyzed: {datetime.now().strftime('%d %b %Y, %H:%M:%S')}")
