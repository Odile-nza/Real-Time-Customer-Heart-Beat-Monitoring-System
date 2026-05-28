import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

DB_URL = "postgresql+psycopg2://admin:admin123@localhost:5434/heartbeat_db"


@st.cache_resource
def get_engine():
    return create_engine(DB_URL)


st.set_page_config(
    page_title="Heartbeat Monitor",
    page_icon="❤️",
    layout="wide"
)

st.title("❤️ Real-Time Customer Heart Beat Monitoring")
st.caption("Live data from Kafka pipeline — updates every 3 seconds")


def load_recent(limit=200):
    df = pd.read_sql(
        "SELECT customer_id, heart_rate, status, timestamp FROM heartbeats ORDER BY timestamp DESC LIMIT %(limit)s",
        get_engine(),
        params={"limit": limit},
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def load_patient_list():
    return pd.read_sql(
        "SELECT DISTINCT customer_id, name FROM heartbeats ORDER BY customer_id",
        get_engine(),
    )


def load_patient_history(customer_id, limit=100):
    df = pd.read_sql(
        "SELECT heart_rate, status, timestamp FROM heartbeats WHERE customer_id = %(cid)s ORDER BY timestamp DESC LIMIT %(limit)s",
        get_engine(),
        params={"cid": customer_id, "limit": limit},
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def load_per_customer():
    return pd.read_sql("""
        SELECT
            customer_id,
            ROUND(AVG(heart_rate)) AS avg_bpm,
            MIN(heart_rate)        AS min_bpm,
            MAX(heart_rate)        AS max_bpm,
            COUNT(*)               AS total,
            SUM(CASE WHEN status = 'anomaly' THEN 1 ELSE 0 END) AS anomalies
        FROM heartbeats
        GROUP BY customer_id
        ORDER BY anomalies DESC
    """, get_engine())


@st.fragment(run_every=3)
def metrics():
    df = load_recent()
    total         = len(df)
    anomaly_count = int((df["status"] == "anomaly").sum())
    normal_count  = int((df["status"] == "normal").sum())
    latest_bpm    = int(df["heart_rate"].iloc[0]) if not df.empty else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total records", f"{total:,}")
    col2.metric("Normal", normal_count)
    col3.metric("Anomalies", anomaly_count)
    col4.metric("Latest BPM", f"{latest_bpm} bpm")


@st.fragment(run_every=3)
def charts():
    df = load_recent()
    per_customer = load_per_customer()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Heart rate over time")
        line_df = df[["timestamp", "heart_rate"]].sort_values("timestamp").set_index("timestamp")
        st.line_chart(line_df, y="heart_rate", height=300)

    with col_right:
        st.subheader("Anomalies per customer")
        bar_df = per_customer[per_customer["anomalies"] > 0][["customer_id", "anomalies"]].set_index("customer_id")
        st.bar_chart(bar_df, height=300)


@st.fragment(run_every=3)
def tables():
    df = load_recent()
    per_customer = load_per_customer()

    st.subheader("Per-customer summary")
    st.dataframe(per_customer, use_container_width=True, hide_index=True)

    st.subheader("Latest 20 records")

    def highlight_anomaly(row):
        color = "background-color: #ffcccc" if row["status"] == "anomaly" else ""
        return [color] * len(row)

    st.dataframe(
        df.head(20).style.apply(highlight_anomaly, axis=1),
        use_container_width=True,
        hide_index=True
    )


@st.fragment(run_every=3)
def patient_detail():
    patients = load_patient_list()
    if patients.empty:
        st.info("No patient data yet.")
        return

    label_map = {
        row.customer_id: f"{row.customer_id} — {row.name}" if row.name else row.customer_id
        for row in patients.itertuples()
    }
    selected_id = st.selectbox(
        "Select patient",
        options=list(label_map.keys()),
        format_func=lambda k: label_map[k],
    )

    df = load_patient_history(selected_id)
    if df.empty:
        st.warning("No readings for this patient.")
        return

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("BPM over time")
        chart_df = (
            df[["timestamp", "heart_rate"]]
            .sort_values("timestamp")
            .set_index("timestamp")
        )
        st.line_chart(chart_df, y="heart_rate", height=300)

    with col_right:
        st.subheader("Reading status distribution")
        dist = (
            df["status"]
            .value_counts()
            .rename_axis("status")
            .reset_index(name="count")
            .set_index("status")
        )
        st.bar_chart(dist, height=300)


# ── Render ──────────────────────────────────────────────────────────────────
metrics()
st.divider()
charts()
st.divider()
st.subheader("Patient detail")
patient_detail()
st.divider()
tables()
