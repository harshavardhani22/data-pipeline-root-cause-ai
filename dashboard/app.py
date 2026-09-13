import os
import sys
from datetime import datetime

import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, DateType

# ============================================================
# PROJECT PATH
# ============================================================

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# ============================================================
# PROJECT MODULES
# ============================================================

from src.validation.schema_detector import detect_schema_drift
from src.detection.data_quality import generate_quality_report
from src.detection.log_detector import detect_log_errors
from src.root_cause.root_cause_engine import analyze_root_cause
from src.root_cause.impact_analysis import find_downstream_impact


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Pipeline Sentinel",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================================
# PROFESSIONAL UI STYLE
# ============================================================

st.markdown(
    """
    <style>

    @import url(
        'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap'
    );

    * {
        font-family: 'Inter', sans-serif;
    }

    .stApp {
        background: #0b0f14;
        color: #e5e7eb;
    }

    [data-testid="stSidebar"] {
        background: #0f141b;
        border-right: 1px solid #202a35;
    }

    .block-container {
        max-width: 1500px;
        padding-top: 2rem;
        padding-bottom: 3rem;
    }

    .hero {
        background: linear-gradient(
            135deg,
            #121a23,
            #0d131a
        );

        border: 1px solid #263342;
        border-radius: 18px;

        padding: 28px 32px;
        margin-bottom: 22px;

        box-shadow:
            0 12px 35px rgba(0,0,0,0.20);
    }

    .eyebrow {
        color: #7dd3fc;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 1.5px;
        margin-bottom: 8px;
    }

    .hero-title {
        color: #f8fafc;
        font-size: 30px;
        font-weight: 700;
    }

    .hero-subtitle {
        color: #94a3b8;
        font-size: 13px;
        margin-top: 7px;
    }

    .panel {
        background: #10161e;
        border: 1px solid #222d3a;
        border-radius: 14px;
        padding: 20px;
        margin-bottom: 18px;
    }

    .panel-title {
        color: #e2e8f0;
        font-size: 14px;
        font-weight: 700;
        margin-bottom: 14px;
    }

    .root-cause {
        background: #151a20;
        border: 1px solid #5b3035;
        border-left: 4px solid #ef4444;
        border-radius: 12px;
        padding: 18px;
    }

    .cause-name {
        color: #f8fafc;
        font-size: 21px;
        font-weight: 700;
    }

    .cause-meta {
        color: #94a3b8;
        font-size: 12px;
        margin-top: 6px;
    }

    .section-title {
        color: #f1f5f9;
        font-size: 18px;
        font-weight: 700;
        margin: 15px 0 12px;
    }

    .list-item {
        color: #cbd5e1;
        padding: 9px 0;
        border-bottom: 1px solid #1d2630;
        font-size: 13px;
    }

    .list-item:last-child {
        border-bottom: none;
    }

    .footer {
        color: #475569;
        font-size: 11px;
        text-align: center;
        margin-top: 30px;
    }

    div[data-testid="stMetric"] {
        background: #111820;
        border: 1px solid #222d3a;
        border-radius: 12px;
        padding: 15px;
    }

    .stButton > button {
        border-radius: 9px;
        border: 1px solid #334155;
        background: #16202b;
        color: #e2e8f0;
        font-weight: 600;
    }

    </style>
    """,
    unsafe_allow_html=True
)


# ============================================================
# SPARK SESSION
# ============================================================

@st.cache_resource
def get_spark():

    return (
        SparkSession.builder
        .appName("PipelineSentinelDashboard")
        .master("local[*]")
        .getOrCreate()
    )


# ============================================================
# RUN COMPLETE ANALYSIS
# ============================================================

@st.cache_data(ttl=60)
def run_analysis():

    spark = get_spark()

    # --------------------------------------------------------
    # Load dataset
    # --------------------------------------------------------

    data_path = os.path.join(
        PROJECT_ROOT,
        "data",
        "raw",
        "sales.csv"
    )

    df = spark.read.csv(
        data_path,
        header=True,
        inferSchema=True
    )

    record_count = df.count()

    # --------------------------------------------------------
    # Expected schema
    # --------------------------------------------------------

    expected_schema = {

        "order_id": IntegerType(),

        "customer_id": IntegerType(),

        "product": StringType(),

        "category": StringType(),

        "quantity": IntegerType(),

        "amount": IntegerType(),

        "date": DateType()

    }

    # --------------------------------------------------------
    # Schema detection
    # --------------------------------------------------------

    schema_issues = detect_schema_drift(
        df,
        expected_schema
    )

    # --------------------------------------------------------
    # Data quality
    # --------------------------------------------------------

    quality_report = generate_quality_report(df)

    # --------------------------------------------------------
    # Logs
    # --------------------------------------------------------

    log_path = os.path.join(
        PROJECT_ROOT,
        "logs",
        "pipeline_failure.log"
    )

    if os.path.exists(log_path):

        with open(
            log_path,
            "r",
            encoding="utf-8"
        ) as file:

            log_text = file.read()

    else:

        log_text = ""

    log_errors = detect_log_errors(
        log_text
    )

    # --------------------------------------------------------
    # Root cause
    # --------------------------------------------------------

    root_cause = analyze_root_cause(

        schema_issues,

        quality_report,

        log_errors

    )

    # --------------------------------------------------------
    # Impact
    # --------------------------------------------------------

    impact = find_downstream_impact(
        "sales_pipeline"
    )

    return {

        "record_count": record_count,

        "schema_issues": schema_issues,

        "quality_report": quality_report,

        "log_errors": log_errors,

        "root_cause": root_cause,

        "impact": impact

    }


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.markdown("## ◈ Pipeline Sentinel")

    st.caption(
        "Data Reliability Console"
    )

    st.divider()

    st.markdown("**Environment**")

    environment = st.selectbox(
        "Environment",
        [
            "Production",
            "Staging",
            "Development"
        ],
        index=0,
        label_visibility="collapsed"
    )

    st.markdown("**Pipeline**")

    pipeline = st.selectbox(
        "Pipeline",
        [
            "sales_pipeline",
            "customer_pipeline",
            "analytics_pipeline"
        ],
        label_visibility="collapsed"
    )

    st.divider()

    st.markdown("**Detection modules**")

    st.checkbox(
        "Schema drift",
        value=True,
        disabled=True
    )

    st.checkbox(
        "Data quality",
        value=True,
        disabled=True
    )

    st.checkbox(
        "Log analysis",
        value=True,
        disabled=True
    )

    st.checkbox(
        "Impact analysis",
        value=True,
        disabled=True
    )

    st.divider()

    if st.button(
        "↻  Refresh analysis",
        use_container_width=True
    ):

        st.cache_data.clear()
        st.rerun()

    st.caption(
        "Analysis refresh interval: 60 seconds"
    )


# ============================================================
# EXECUTE ANALYSIS
# ============================================================

try:

    data = run_analysis()

except Exception as error:

    st.error(
        "Pipeline analysis failed."
    )

    st.exception(error)

    st.stop()


schema_issues = data["schema_issues"]

quality_report = data["quality_report"]

log_errors = data["log_errors"]

root_cause = data["root_cause"]

impact = data["impact"]

record_count = data["record_count"]


# ============================================================
# DERIVED METRICS
# ============================================================

pipeline_failed = (
    bool(schema_issues)
    or bool(log_errors)
)

pipeline_status = (
    "FAILED"
    if pipeline_failed
    else "HEALTHY"
)

duplicate_count = (
    quality_report["duplicates"]["duplicate_count"]
)

negative_amounts = (
    quality_report["negative_values"]["negative_amounts"]
)

negative_quantities = (
    quality_report["negative_values"]["negative_quantities"]
)

affected_pipelines = (
    len(impact["affected_pipelines"])
)

affected_dashboards = (
    len(impact["affected_dashboards"])
)

last_checked = datetime.now().strftime(
    "%d %b %Y, %H:%M:%S"
)


# ============================================================
# HEADER
# ============================================================

st.markdown(
    f"""
    <div class="hero">

        <div class="eyebrow">
            DATA RELIABILITY / {environment.upper()}
        </div>

        <div class="hero-title">
            Pipeline Incident Overview
        </div>

        <div class="hero-subtitle">
            Automated investigation for
            <b>{pipeline}</b>
            across schema, data quality,
            logs and downstream dependencies.
        </div>

    </div>
    """,
    unsafe_allow_html=True
)


# ============================================================
# KPI CARDS
# ============================================================

k1, k2, k3, k4 = st.columns(4)

with k1:

    st.metric(
        "Pipeline Status",
        pipeline_status
    )

with k2:

    st.metric(
        "Root Cause",
        root_cause["root_cause"]
    )

with k3:

    st.metric(
        "Confidence",
        f"{root_cause['confidence']}%"
    )

with k4:

    st.metric(
        "Records Analyzed",
        record_count
    )


# ============================================================
# INCIDENT DIAGNOSIS
# ============================================================

st.markdown(
    '<div class="section-title">Incident Diagnosis</div>',
    unsafe_allow_html=True
)

left, right = st.columns(
    [1.3, 1]
)


with left:

    st.markdown(
        f"""
        <div class="root-cause">

            <div class="cause-name">
                ◉ {root_cause["root_cause"]}
            </div>

            <div class="cause-meta">
                Confidence {root_cause["confidence"]}%
                &nbsp;•&nbsp;
                Severity {root_cause["severity"]}
                &nbsp;•&nbsp;
                Pipeline {pipeline}
            </div>

        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown("")

    tabs = st.tabs(
        [
            "Schema",
            "Data Quality",
            "Logs"
        ]
    )

    # --------------------------------------------------------
    # Schema
    # --------------------------------------------------------

    with tabs[0]:

        if schema_issues:

            for issue in schema_issues:

                st.error(
                    f"**{issue['column']}**  |  "
                    f"{issue['expected']} → "
                    f"{issue['actual']}"
                )

        else:

            st.success(
                "Schema validation passed."
            )

    # --------------------------------------------------------
    # Data quality
    # --------------------------------------------------------

    with tabs[1]:

        q1, q2, q3 = st.columns(3)

        with q1:

            st.metric(
                "Duplicates",
                duplicate_count
            )

        with q2:

            st.metric(
                "Negative Amounts",
                negative_amounts
            )

        with q3:

            st.metric(
                "Negative Quantities",
                negative_quantities
            )

    # --------------------------------------------------------
    # Logs
    # --------------------------------------------------------

    with tabs[2]:

        if log_errors:

            for error in log_errors:

                st.warning(
                    f"**{error['error_type']}** — "
                    f"{error.get('message', 'Detected')}"
                )

        else:

            st.success(
                "No relevant pipeline errors detected."
            )


# ============================================================
# RECOMMENDATION + EVIDENCE
# ============================================================

with right:

    st.markdown(
        '<div class="panel">',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="panel-title">Recommended Action</div>',
        unsafe_allow_html=True
    )

    st.write(
        root_cause["recommendation"]
    )

    st.markdown(
        "</div>",
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="panel">',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="panel-title">Evidence Scoring</div>',
        unsafe_allow_html=True
    )

    for cause, score in root_cause[
        "evidence_scores"
    ].items():

        st.write(
            f"**{cause}** — {score}"
        )

        st.progress(
            min(score, 100) / 100
        )

    st.markdown(
        "</div>",
        unsafe_allow_html=True
    )


# ============================================================
# DOWNSTREAM IMPACT
# ============================================================

st.markdown(
    '<div class="section-title">Downstream Impact</div>',
    unsafe_allow_html=True
)

i1, i2, i3 = st.columns(3)

with i1:

    st.metric(
        "Affected Pipelines",
        affected_pipelines
    )

with i2:

    st.metric(
        "Affected Dashboards",
        affected_dashboards
    )

with i3:

    if affected_pipelines >= 2:

        impact_level = "HIGH"

    elif affected_pipelines == 1:

        impact_level = "MEDIUM"

    else:

        impact_level = "LOW"

    st.metric(
        "Impact Level",
        impact_level
    )


# ============================================================
# DEPENDENCY DETAILS
# ============================================================

d1, d2 = st.columns(2)


with d1:

    st.markdown(
        '<div class="panel">',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="panel-title">Affected Pipelines</div>',
        unsafe_allow_html=True
    )

    for item in impact[
        "affected_pipelines"
    ]:

        st.markdown(
            f'<div class="list-item">▸ {item}</div>',
            unsafe_allow_html=True
        )

    st.markdown(
        "</div>",
        unsafe_allow_html=True
    )


with d2:

    st.markdown(
        '<div class="panel">',
        unsafe_allow_html=True
    )

    st.markdown(
        '<div class="panel-title">Affected Dashboards</div>',
        unsafe_allow_html=True
    )

    for item in impact[
        "affected_dashboards"
    ]:

        st.markdown(
            f'<div class="list-item">▸ {item}</div>',
            unsafe_allow_html=True
        )

    st.markdown(
        "</div>",
        unsafe_allow_html=True
    )


# ============================================================
# INCIDENT SUMMARY
# ============================================================

st.markdown(
    '<div class="section-title">Incident Summary</div>',
    unsafe_allow_html=True
)


summary = {

    "Signal": [
        "Schema validation",
        "Data quality",
        "Pipeline logs",
        "Downstream dependencies"
    ],

    "Status": [

        "FAILED"
        if schema_issues
        else "PASSED",

        "PASSED"
        if (
            duplicate_count == 0
            and negative_amounts == 0
            and negative_quantities == 0
        )
        else "WARNING",

        "FAILED"
        if log_errors
        else "PASSED",

        "IMPACTED"
        if (
            affected_pipelines > 0
            or affected_dashboards > 0
        )
        else "NONE"
    ],

    "Finding": [

        (
            f"{schema_issues[0]['column']}: "
            f"{schema_issues[0]['expected']} → "
            f"{schema_issues[0]['actual']}"
        )
        if schema_issues
        else "No schema drift detected",

        (
            "No duplicate or negative-value issues"
            if (
                duplicate_count == 0
                and negative_amounts == 0
                and negative_quantities == 0
            )
            else "Data quality violations detected"
        ),

        f"{len(log_errors)} relevant log errors",

        (
            f"{affected_pipelines} pipelines / "
            f"{affected_dashboards} dashboards"
        )
    ]
}


st.dataframe(
    summary,
    use_container_width=True,
    hide_index=True
)


# ============================================================
# FOOTER
# ============================================================

st.markdown(
    f"""
    <div class="footer">

        Pipeline Sentinel ·
        Last analysis: {last_checked}
        · PySpark · Python · Streamlit ·
        Airflow · Scikit-learn · AWS

    </div>
    """,
    unsafe_allow_html=True
)