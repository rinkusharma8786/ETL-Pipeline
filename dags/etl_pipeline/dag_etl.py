
import os
from datetime import datetime
import sqlite3
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from kaggle.api.kaggle_api_extended import KaggleApi


# -----------------------------
# PATHS
# -----------------------------
DATA_DIR = "/opt/airflow/dags/etl_pipeline/data"
OUTPUT_DIR = "/opt/airflow/data"
RAW_FILE = f"{DATA_DIR}/spy_sample-1.csv"
CLEAN_FILE = f"{OUTPUT_DIR}/cleaned.csv"
DB_FILE = f"{OUTPUT_DIR}/stocks.db"
CLEAN_FULL_FILE = f"{OUTPUT_DIR}/cleaned_full.csv"



# extract dataset

def extract_data():

    dataset = "sashagolovin/option-chain-field-price-prediction"

    os.makedirs(DATA_DIR, exist_ok=True)

    if os.listdir(DATA_DIR):
        print("Dataset already exists. Skipping download.")
        return

    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(
        dataset,
        path=DATA_DIR,
        unzip=True
    )

    print("Dataset downloaded successfully")



# trasnform
def transform_data():

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Reading dataset...")

    df = pd.read_csv(RAW_FILE)


    # Dataset inspection
    
    print("Dataset Shape:", df.shape)
    print("Columns:", df.columns.tolist())

    
    # Data validation

    if df.empty:
        raise ValueError("Dataset is empty")

    required_columns = [
        "QUOTE_UNIXTIME",
        "STRIKE",
        "UNDERLYING_LAST",
        "C_IV",
        "P_IV",
        "C_VOLUME",
        "P_VOLUME"
    ]

    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    
    # Cleaning
    
    print("Cleaning dataset...")

    before_rows = df.shape[0]

    df = df.drop_duplicates()
    df = df.dropna()

    after_rows = df.shape[0]

    print(f"Removed {before_rows - after_rows} dirty rows")

    # Save cleaned dataset
    
    df.to_csv(CLEAN_FULL_FILE, index=False)

    
    # Convert Unix Time
    
    print("Converting timestamp...")

    df["time"] = pd.to_datetime(df["QUOTE_UNIXTIME"], unit="s")

    
    # ATM IV Calculation
    
    print("Calculating ATM IV...")

    df["distance"] = abs(df["STRIKE"] - df["UNDERLYING_LAST"])

    atm_df = df.loc[df.groupby("QUOTE_UNIXTIME")["distance"].idxmin()].copy()

    atm_df["ATM_CALL_IV"] = atm_df["C_IV"]
    atm_df["ATM_PUT_IV"] = atm_df["P_IV"]
    atm_df["ATM_IV"] = (atm_df["C_IV"] + atm_df["P_IV"]) / 2

    # -------------------------
    # Put Call Ratio
    # -------------------------
    print("Calculating Put-Call Ratio...")

    total_put_volume = df["P_VOLUME"].sum()
    total_call_volume = df["C_VOLUME"].sum()

    if total_call_volume == 0:
        pcr = None
        print("Call volume is zero, PCR cannot be calculated")
    else:
        pcr = total_put_volume / total_call_volume

    print("Put Call Ratio:", pcr)

    
    # Most Active Strikes
    
    print("Finding most active strikes...")

    df["total_volume"] = df["C_VOLUME"] + df["P_VOLUME"]

    active_strikes = (
        df.groupby("STRIKE")["total_volume"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )

    print("Top 10 Most Active Strikes:")
    print(active_strikes)

    # -------------------------
    # Save ATM dataset
    # -------------------------
    atm_df.to_csv(CLEAN_FILE, index=False)

    print("Transformation complete")


# -----------------------------
# LOAD
# -----------------------------
def load_data():

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Connecting to SQLite database...")

    conn = sqlite3.connect(DB_FILE)

    
    # Load cleaned dataset

    cleaned_full_path = f"{OUTPUT_DIR}/cleaned_full.csv"

    df_clean = pd.read_csv(cleaned_full_path)

    print("Loading cleaned data into SQLite...")

    df_clean.to_sql(
        "cleaned_data",
        conn,
        if_exists="replace",
        index=False
    )

    print("Cleaned data loaded")

    
    # Load ATM dataset in chunks
    
    print("Loading ATM dataset in chunks...")

    chunk_size = 5000
    first_chunk = True

    for chunk in pd.read_csv(CLEAN_FILE, chunksize=chunk_size):

        if first_chunk:
            chunk.to_sql(
                "atm_data",
                conn,
                if_exists="replace",
                index=False
            )
            first_chunk = False
        else:
            chunk.to_sql(
                "atm_data",
                conn,
                if_exists="append",
                index=False
            )

    conn.close()

    print("ATM data loaded into SQLite successfully")


# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id="etl_kaggle_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task