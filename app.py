import streamlit as st
import pandas as pd
import sqlite3
import os

# -----------------------------
# AUTO REFRESH (every 60 sec)
# -----------------------------
from streamlit_autorefresh import st_autorefresh

st_autorefresh(interval=60000, key="auto_refresh")


# -----------------------------
# PAGE CONFIG
# -----------------------------
st.set_page_config(git
    page_title="Options Dashboard",
    layout="wide"
)

st.title("📊 Options Market Dashboard")


# -----------------------------
# DATABASE PATH
# -----------------------------
DB_PATH = "data/stocks.db"   # adjust if needed


# -----------------------------
# LOAD DATA (with caching)
# -----------------------------
@st.cache_data
def load_data():
    conn = sqlite3.connect(DB_PATH)

    atm_df = pd.read_sql("SELECT * FROM atm_data", conn)
    clean_df = pd.read_sql("SELECT * FROM cleaned_data", conn)

    conn.close()
    return atm_df, clean_df


atm_df, clean_df = load_data()


# -----------------------------
# SIDEBAR FILTER
# -----------------------------
st.sidebar.header("Filters")

if "QUOTE_UNIXTIME" in clean_df.columns:
    unique_times = sorted(clean_df["QUOTE_UNIXTIME"].unique())
    selected_time = st.sidebar.selectbox("Select Timestamp", unique_times)

    filtered_df = clean_df[clean_df["QUOTE_UNIXTIME"] == selected_time]
else:
    filtered_df = clean_df


# -----------------------------
# KPI SECTION
# -----------------------------
st.subheader("📌 Key Metrics")

col1, col2 = st.columns(2)

# Put-Call Ratio
total_put = clean_df["P_VOLUME"].sum()
total_call = clean_df["C_VOLUME"].sum()

pcr = total_put / total_call if total_call != 0 else 0
col1.metric("Put-Call Ratio", round(pcr, 2))

# ATM IV
avg_atm_iv = atm_df["ATM_IV"].mean()
col2.metric("Average ATM IV", round(avg_atm_iv, 4))


# -----------------------------
# ATM IV TREND
# -----------------------------
st.subheader("📈 ATM Implied Volatility Trend")

if "time" in atm_df.columns:
    atm_df["time"] = pd.to_datetime(atm_df["time"])
    atm_df = atm_df.sort_values("time")
    st.line_chart(atm_df.set_index("time")["ATM_IV"])
else:
    st.line_chart(atm_df["ATM_IV"])


# -----------------------------
# VOLATILITY SMILE
# -----------------------------
st.subheader("📉 Volatility Smile")

if not filtered_df.empty:
    smile_df = filtered_df.sort_values("STRIKE")
    st.line_chart(smile_df.set_index("STRIKE")[["C_IV", "P_IV"]])
else:
    st.warning("No data available for selected time")


# -----------------------------
# MOST ACTIVE STRIKES
# -----------------------------
st.subheader("🔥 Most Active Strikes")

clean_df["total_volume"] = clean_df["C_VOLUME"] + clean_df["P_VOLUME"]

top_strikes = (
    clean_df.groupby("STRIKE")["total_volume"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

st.bar_chart(top_strikes)


# -----------------------------
# VOLUME DISTRIBUTION
# -----------------------------
st.subheader("📊 Volume Distribution")

volume_df = clean_df[["C_VOLUME", "P_VOLUME"]].sum()

st.bar_chart(volume_df)


# -----------------------------
# SMART DB CHANGE DETECTION
# -----------------------------
st.subheader("🔄 Data Status")

if os.path.exists(DB_PATH):
    last_modified = os.path.getmtime(DB_PATH)
    st.info(f"Last Data Update: {pd.to_datetime(last_modified, unit='s')}")
else:
    st.error("Database not found!")


# -----------------------------
# MANUAL REFRESH BUTTON
# -----------------------------
if st.button("🔄 Refresh Now"):
    st.cache_data.clear()
    st.rerun()