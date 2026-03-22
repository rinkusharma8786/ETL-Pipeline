import os
import logging
from datetime import datetime, timedelta
import sqlite3
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from kaggle.api.kaggle_api_extended import KaggleApi


# DEFAULT ARGS (Retries + Alerts)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email": ["rinkusharma1770@gmail.com"], 
    "email_on_failure": True,
    "email_on_retry": True,
}




# PATHS

DATA_DIR = "/opt/airflow/dags/etl_pipeline/data"
OUTPUT_DIR = "/opt/airflow/data"
RAW_FILE = f"{DATA_DIR}/spy_sample-1.csv"    
CLEAN_FILE = f"{OUTPUT_DIR}/cleaned.csv"
CLEAN_FULL_FILE = f"{OUTPUT_DIR}/cleaned_full.csv"
DB_FILE = f"{OUTPUT_DIR}/stocks.db"



# EXTRACT

def extract_data():
    dataset = "sashagolovin/option-chain-field-price-prediction"

    os.makedirs(DATA_DIR, exist_ok=True)

    if os.listdir(DATA_DIR):
        logging.info("Dataset already exists. Skipping download.")
        return

    try:
        api = KaggleApi()
        api.authenticate()

        api.dataset_download_files(
            dataset,
            path=DATA_DIR,
            unzip=True
        )

        logging.info("Dataset downloaded successfully")

    except Exception as e:
        logging.error(f"Error in extract_data: {e}")
        raise


# TRANSFORM

def transform_data():
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        df = pd.read_csv(RAW_FILE)
        logging.info(f"Raw data shape: {df.shape}")

        
        # Validation
        
        if df.empty:
            raise ValueError("Dataset is empty")

        required_cols = ["STRIKE", "C_VOLUME", "P_VOLUME"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing column: {col}")

        
        # Cleaning
        
        before_rows = df.shape[0]

        df = df.drop_duplicates()
        df = df.dropna()

        after_rows = df.shape[0]
        logging.info(f"Removed {before_rows - after_rows} rows during cleaning")

        df.to_csv(CLEAN_FULL_FILE, index=False)

        
        # Time Conversion
        
        df["time"] = pd.to_datetime(df["QUOTE_UNIXTIME"], unit="s")

        
        # ATM IV
        
        df["distance"] = abs(df["STRIKE"] - df["UNDERLYING_LAST"])

        atm_df = df.loc[df.groupby("QUOTE_UNIXTIME")["distance"].idxmin()].copy()

        atm_df["ATM_IV"] = (atm_df["C_IV"] + atm_df["P_IV"]) / 2

    
        # PCR
        
        total_put_volume = df["P_VOLUME"].sum()
        total_call_volume = df["C_VOLUME"].sum()

        if total_call_volume == 0:
            logging.error("Call volume is zero, cannot compute PCR")
            pcr = None
        else:
            pcr = total_put_volume / total_call_volume

        logging.info(f"Put-Call Ratio: {pcr}")

        
        # Most Active Strikes
        
        df["total_volume"] = df["C_VOLUME"] + df["P_VOLUME"]

        active_strikes = (
            df.groupby("STRIKE")["total_volume"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )

        logging.info("Top 10 Active Strikes:")
        logging.info(active_strikes.to_string())

        
        # Save ATM data
    
        atm_df.to_csv(CLEAN_FILE, index=False)

        logging.info("Transformation completed successfully")

    except Exception as e:
        logging.error(f"Error in transform_data: {e}")
        raise



# LOAD

def load_data():
    try:
        conn = sqlite3.connect(DB_FILE)

        
        # Load cleaned full data (chunked)
        
        chunk_size = 5000
        first_chunk = True

        for chunk in pd.read_csv(CLEAN_FULL_FILE, chunksize=chunk_size):

            if first_chunk:
                chunk.to_sql(
                    "cleaned_data",
                    conn,
                    if_exists="replace",
                    index=False
                )
                first_chunk = False
            else:
                chunk.to_sql(
                    "cleaned_data",
                    conn,
                    if_exists="append",
                    index=False
                )

        logging.info("Cleaned data loaded (chunked)")

        
        # Load ATM data (chunked)
        
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

        logging.info("ATM data loaded into SQLite")

    except Exception as e:
        logging.error(f"Error in load_data: {e}")
        raise



# DAG

with DAG(
    dag_id="etl_kaggle_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="30 3 * * *", 
    catchup=False,
    default_args=default_args,
    tags=["etl", "finance"],
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