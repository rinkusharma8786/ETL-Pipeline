

import streamlit as st
import pandas as pd
import sqlite3
import os

# AUTO REFRESH
from streamlit_autorefresh import st_autorefresh


# AUTO REFRESH (every 60 sec)

st_autorefresh(interval=60000, key="auto_refresh")


# PAGE CONFIG

st.set_page_config(
    page_title="Options Dashboard",
    layout="wide"
)

st.title("📊 Options Market Dashboard")
st.markdown("Interactive dashboard for analyzing SPY options market data")


# DATABASE PATH

DB_PATH = "data/stocks.db"


# LOAD DATA

@st.cache_data
def load_data():
    conn = sqlite3.connect(DB_PATH)

    atm_df = pd.read_sql("SELECT * FROM atm_data", conn)
    clean_df = pd.read_sql("SELECT * FROM cleaned_data", conn)

    conn.close()
    return atm_df, clean_df


# CHECK DB

if not os.path.exists(DB_PATH):
    st.error("❌ Database not found! Please run the Airflow pipeline first.")
    st.stop()

atm_df, clean_df = load_data()


# DATETIME CONVERSION

if "QUOTE_UNIXTIME" in clean_df.columns:
    clean_df["time"] = pd.to_datetime(clean_df["QUOTE_UNIXTIME"], unit="s")

if "time" in atm_df.columns:
    atm_df["time"] = pd.to_datetime(atm_df["time"])


# SIDEBAR FILTERS

st.sidebar.header("Filters")

if "time" in clean_df.columns:
    unique_times = sorted(clean_df["time"].drop_duplicates())
    selected_time = st.sidebar.selectbox("Select Timestamp", unique_times)

    filtered_df = clean_df[clean_df["time"] == selected_time]
else:
    filtered_df = clean_df
    selected_time = None


# DATA STATUS

st.sidebar.subheader("Data Status")
last_modified = os.path.getmtime(DB_PATH)
st.sidebar.info(f"Last Update:\n{pd.to_datetime(last_modified, unit='s')}")

if st.sidebar.button("🔄 Refresh Now"):
    st.cache_data.clear()
    st.rerun()


# KPI SECTION

st.subheader("📌 Key Metrics")

col1, col2, col3 = st.columns(3)

# Put-Call Ratio
total_put = clean_df["P_VOLUME"].sum()
total_call = clean_df["C_VOLUME"].sum()
pcr = total_put / total_call if total_call != 0 else 0
col1.metric("Put-Call Ratio", round(pcr, 2))

# Average ATM IV
avg_atm_iv = atm_df["ATM_IV"].mean() if "ATM_IV" in atm_df.columns else 0
col2.metric("Average ATM IV", round(avg_atm_iv, 4))

# Total Records
col3.metric("Total Records", len(clean_df))


# ATM IV TREND

st.subheader("📈 ATM Implied Volatility Trend")

if not atm_df.empty and "time" in atm_df.columns and "ATM_IV" in atm_df.columns:
    atm_plot = atm_df.sort_values("time").set_index("time")
    st.line_chart(atm_plot["ATM_IV"])
else:
    st.warning("ATM IV trend data not available.")


# VOLATILITY SMILE

st.subheader("📉 Volatility Smile")

if not filtered_df.empty and {"STRIKE", "C_IV", "P_IV"}.issubset(filtered_df.columns):
    smile_df = filtered_df.sort_values("STRIKE")
    st.line_chart(smile_df.set_index("STRIKE")[["C_IV", "P_IV"]])
else:
    st.warning("No volatility smile data available for selected timestamp.")


# MOST ACTIVE STRIKES (Selected Timestamp)

st.subheader("🔥 Most Active Strikes (Selected Timestamp)")

if not filtered_df.empty and {"STRIKE", "C_VOLUME", "P_VOLUME"}.issubset(filtered_df.columns):
    filtered_df = filtered_df.copy()
    filtered_df["total_volume"] = filtered_df["C_VOLUME"] + filtered_df["P_VOLUME"]

    top_strikes = (
        filtered_df.groupby("STRIKE")["total_volume"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )

    st.bar_chart(top_strikes)
else:
    st.warning("No strike activity data available.")


# VOLUME DISTRIBUTION

st.subheader("📊 Volume Distribution")

if {"C_VOLUME", "P_VOLUME"}.issubset(clean_df.columns):
    volume_df = clean_df[["C_VOLUME", "P_VOLUME"]].sum()
    st.bar_chart(volume_df)
else:
    st.warning("Volume data not available.")

# GREEKS BEHAVIOR

st.subheader("🧠 Greeks Behavior (Selected Timestamp)")

greek_options = []

if "C_DELTA" in filtered_df.columns:
    greek_options.append("C_DELTA")
if "P_DELTA" in filtered_df.columns:
    greek_options.append("P_DELTA")
if "C_GAMMA" in filtered_df.columns:
    greek_options.append("C_GAMMA")
if "P_GAMMA" in filtered_df.columns:
    greek_options.append("P_GAMMA")
if "C_VEGA" in filtered_df.columns:
    greek_options.append("C_VEGA")
if "P_VEGA" in filtered_df.columns:
    greek_options.append("P_VEGA")
if "C_THETA" in filtered_df.columns:
    greek_options.append("C_THETA")
if "P_THETA" in filtered_df.columns:
    greek_options.append("P_THETA")

if greek_options and "STRIKE" in filtered_df.columns:
    selected_greeks = st.multiselect(
        "Select Greeks to View",
        greek_options,
        default=greek_options[:2]
    )

    if selected_greeks:
        greeks_df = filtered_df.sort_values("STRIKE").set_index("STRIKE")[selected_greeks]
        st.line_chart(greeks_df)
    else:
        st.info("Select at least one Greek to display.")
else:
    st.warning("Greeks data not available in this dataset.")


# OPTION CHAIN SNAPSHOT

st.subheader("📋 Option Chain Snapshot")

snapshot_cols = [
    "time", "STRIKE", "UNDERLYING_LAST",
    "C_BID", "C_ASK", "P_BID", "P_ASK",
    "C_VOLUME", "P_VOLUME", "C_IV", "P_IV"
]

available_cols = [col for col in snapshot_cols if col in filtered_df.columns]

if not filtered_df.empty and available_cols:
    st.dataframe(
        filtered_df[available_cols].sort_values("STRIKE"),
        use_container_width=True
    )
else:
    st.warning("No option chain snapshot available.")


# SUMMARY INSIGHTS

st.subheader("📌 Dashboard Insights")

sentiment = "Bearish" if pcr > 1 else "Bullish"

st.markdown(f"""
- **Put-Call Ratio:** `{round(pcr, 2)}` → **{sentiment} Market Sentiment**
- **Average ATM IV:** `{round(avg_atm_iv, 4)}`
- **Selected Timestamp:** `{selected_time if selected_time is not None else 'N/A'}`

""")