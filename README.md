# Options Data ETL Pipeline with Orchestration and Monitoring

## Introduction

This project implements an end-to-end ETL (Extract–Transform–Load) pipeline for processing options market data. The pipeline extracts SPY options chain data from Kaggle, performs data cleaning and transformation using Python, and stores the processed data in a structured SQLite database.

Apache Airflow is used for workflow orchestration, scheduling, and monitoring. The system also includes logging, retry mechanisms, and alerting to ensure reliability and fault tolerance.

The project generates key financial insights such as ATM implied volatility, put-call ratio, and most active option strikes, which are widely used in options trading and market analysis.


# Environment Setup & Pipeline Design (Milestone 1)

## 1. Overview

In this milestone, the foundation of the project was established by setting up the development environment using Docker and Apache Airflow, and designing the ETL pipeline architecture.

The goal was to prepare a scalable and production-ready data pipeline by containerizing services, defining data flow, and setting up extraction mechanisms.

---

## 2. Objectives

The main objectives of this milestone were:

- Set up the development environment using Docker  
- Install and configure Apache Airflow using containers  
- Design the ETL pipeline architecture  
- Define data flow from source to storage  
- Prepare data extraction scripts  

---

## 3. Environment Setup

The following tools and technologies were installed and configured:

| Tool | Purpose |
|------|--------|
| Python | Core programming language |
| Docker | Containerization of services |
| Apache Airflow | Workflow orchestration |
| Pandas | Data processing |
| Kaggle API | Dataset extraction |
| SQLite | Data storage |

---

## 4. Apache Airflow Setup (Docker-Based)

Apache Airflow was set up using Docker for better portability and environment consistency.

### Steps Performed

- Installed Docker and Docker Compose  
- Used official Airflow Docker image  
- Configured Airflow services (webserver, scheduler)  
- Mounted project directories into containers  
- Started services using Docker Compose  

### Purpose

- Ensure consistent environment across systems  
- Simplify setup and dependency management  
- Enable scalable and production-like deployment  

---

## 5. ETL Pipeline Design

The ETL pipeline architecture was designed before implementation.

### Pipeline Flow
```
Data Source (Kaggle)
↓
Extract
↓
Transform
↓
Load
↓
Database (SQLite)
```


---

## 6. Key Achievements

This milestone successfully delivered:

- Containerized development environment using Docker  
- Fully functional Apache Airflow setup  
- Designed ETL pipeline architecture  
- Defined data flow from extraction to storage  
- Prepared the system for pipeline implementation  

---


# Pipeline Orchestration and Monitoring (Milestone 2)

## 1. Overview

In this milestone, an ETL (Extract–Transform–Load) pipeline was developed to process options market data from Kaggle.
The pipeline automates the process of downloading, cleaning, transforming, and storing financial options data for further analysis.

The pipeline is orchestrated using Apache Airflow and processes SPY options chain data to derive useful market insights.

---

## 2. Objectives

The main objectives of this milestone were:

- Develop a data cleaning workflow for raw options data
- Implement data transformation using Python (Pandas)
- Extract useful market indicators from the dataset
- Build a reliable and automated ETL pipeline
- Store processed data in a structured database

---

## 3. Dataset Description

The dataset used in this project is from Kaggle:

Dataset Name:

`option-chain-field-price-prediction`

It contains SPY options chain market data, which includes information about call and put options for different strike prices.

Important Columns in the Dataset
| Column | Description |
|------------------|--------------------------------------------------|
| QUOTE_UNIXTIME | Timestamp of the option quote |
| UNDERLYING_LAST | Price of the underlying asset (SPY) |
| STRIKE | Option strike price |
| C_BID / C_ASK | Call option bid and ask prices |
| P_BID / P_ASK | Put option bid and ask prices |
| C_VOLUME | Call option trading volume |
| P_VOLUME | Put option trading volume |
| C_IV | Call option implied volatility |
| P_IV | Put option implied volatility |
| Option Greeks | Risk metrics (Delta, Gamma, Vega, Theta) |

This dataset allows analysis of options market activity and trading behavior

---

## 4. Pipeline Architecture

The pipeline follows the ETL architecture:

```
Kaggle Dataset
       │
       ▼
Extract Data
       │
       ▼
Clean Raw Data
       │
       ▼
Transform Data
       │
       ▼
Generate Market Insights
       │
       ▼
Store in SQLite Database

```

The workflow is automated using Apache Airflow DAG.

---

## 5. Extract Phase

The extraction stage downloads the dataset from Kaggle using the Kaggle API.

Key steps:

1. Authenticate with Kaggle API
2. Download the dataset
3. Store it in the project data directory

Output location:

` /opt/airflow/dags/etl_pipeline/data`

---

## 6. Data Cleaning Phase

The raw dataset may contain duplicates or missing values.
To ensure data quality, the following cleaning steps were implemented.

### Duplicate Removal

Duplicate records are removed to avoid repeated data.

### Missing Value Removal

Rows containing missing values are removed to maintain data integrity.

### Dataset Validation

The pipeline verifies:

- Dataset is not empty
- Required columns exist

These checks ensure the pipeline does not process corrupted data.

---

## 7. Data Transformation

After cleaning the dataset, several transformations were applied to extract useful insights.

### 7.1 Timestamp Conversion

The dataset contains time in UNIX format.
It was converted to a readable datetime format.

Purpose:

- Enables time-based analysis
- Improves data readability

---

### 7.2 At-The-Money (ATM) Implied Volatility

ATM options are those whose strike price is closest to the underlying asset price.

Steps performed:

1. Calculate distance between strike price and underlying price
2. Identify the closest strike
3. Compute ATM implied volatility

Metric created:

`ATM_IV = (Call_IV + Put_IV) / 2`

Purpose:

- Measures expected market volatility
- Widely used in options trading analysis

---

### 7.3 Put-Call Ratio (PCR)

Put-Call Ratio measures market sentiment.

Formula:
`PCR = Total Put Volume / Total Call Volume`

Interpretation:
PCR Value Market Sentiment

| Ratio Value | Market Sentiment  |
| ----------- | ----------------- |
| >1          | Bearish Sentiment |
| <1          | Bullish Sentiment |

Purpose:

- Helps understand trader expectations in the market.

---

### 7.4 Most Active Option Strikes

Trading activity was analyzed by calculating total option volume.

Formula:
` Total Volume = Call Volume + Put Volume`

The pipeline identifies the top 10 most active strike prices.

Purpose:

- Identify key trading levels
- Detect highly liquid options

These strikes often act as support or resistance levels

---

## 8. Load Phase

The processed data is stored in a SQLite database.

Database file:

`stocks.db`

Two tables are created.

**Table 1: cleaned_data**

Contains the fully cleaned dataset.

Purpose:

- Preserve processed raw data
- Allow further analysis.

**Table 2: atm_data**

Contains only ATM option records.

Purpose:

- Focused dataset for volatility analysis.

Chunk loading was used to efficiently insert large datasets.

---

## 9. Pipeline Orchestration with Airflow

The pipeline is automated using Apache Airflow.

Three tasks were created:

1. Extract Data
2. Transform Data
3. Load Data

Task dependency:

```text
extract_data → transform_data → load_data
```

This ensures the pipeline runs in the correct order.

---

## 10. Pipeline Testing and Validation

Several validation checks were implemented.

### Dataset Validation

Ensures dataset is not empty.

### Schema Validation

Verifies required columns exist.

### Cleaning Verification

Tracks number of removed rows.

### Error Handling

Prevents division errors when calculating Put-Call Ratio.

> These checks ensure the pipeline runs reliably.

---

## 11. Outputs Generated

The pipeline generates the following outputs:

### Files Generated

```
cleaned_full.csv
cleaned.csv
stocks.db
```

### Database Tables created

```
cleaned_data
atm_data
```

### Market Insights

- ATM implied volatility
- Put-call ratio
- Most active option strikes

---

## 12. Technologies Used

| Technology     | Purpose                          |
| -------------- | -------------------------------- |
| Python         | Core programming language        |
| Pandas         | Data cleaning and transformation |
| Apache Airflow | Pipeline orchestration           |
| Kaggle API     | Dataset extraction               |
| SQLite         | Data storage                     |

---

## 13. Key Achievements

This milestone successfully delivered:

- Automated ETL pipeline
- Cleaned financial dataset
- Extracted market insights
- Structured database storage
- Workflow orchestration using Airflow


---

# Data Cleaning and Transformation Pipeline (Milestone 3)

# 1.Overview

In this milestone, the existing ETL pipeline was enhanced by adding workflow orchestration,monitoring and fault tolerence.

The pipeline is managed by using Apache Airflow,enabling automated execution,tracking , and failure handling.

---

# 2. Objectives.

The main objectives of this milestone were:

- Automate pipeline exwcution using scheduling
- Implement logging for debugging and tracking
- Add retry mechanisms for fault tolerance
- Configure alerting for failure notifications
- Enable monitoring using Airflow UI

---

# 3. Pipeline Enhancements 

The following enhancements were added to the pipeline

| Feature        | Description                      |
| -------------- | -------------------------------- |
| Scheduling     | Automatically runs the pipeline at fixed intervals |
| Logging         | Tracks exwcution details and errors|
| Retries        | Re-runs failed tasks automatically           |
| Alerts   | Send notifications on failure              |
| Monitoring | Tracks pipeline performance and execution status|

---

# 4. DAG Configuration

The pipeline is defined as an Airflow DAG (Directed Acyclic Graph).

```
with DAG(
    dag_id="etl_kaggle_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="30 3 * * *", 
    catchup=False,
    default_args=default_args,
    tags=["etl", "finance"],
) as dag:

```

---

# 5. Scheduling 

The pipeline is scheduled to run automatically using Airflow.

It will runs daily at:
03:30 + 5:30 = 09:00 AM IST

```
schedule="30 3 * * *"  
```
### Purpose 
- Eliminates manual execution
- Ensures regular data updates 
- Supports production workflows

---

# 6. Logging

Logging was added using Python's logging module.

```
import logging

logging.info("Transformation completed successfully")
logging.error("Error in transform_data")

```

### Purpose
- Tracking pipeline execution
- Debug failures
- Monitor data Processing steps

Logs can be viewed in the Airflow UI.

---

# 7. Retry Mechanisms

Retry logic was added to handle temporary failures.

```
default_args={
       "retries":2,
       "retry_delay":timedelta(minutes=5),
}

```

### Purpose
- Automatically retry failed tasks
- Improve pipeline reliability 
- Handle transient error

---


# 8. Alerting System

Email alerts were configued for failures and retries

```
default_args = {
  
    "email": ["rinkusharma1770@gmail.com"], 
    "email_on_failure": True,
    "email_on_retry": True,
}

```

### Purpose
- Notify users when pipeline fails
- Enable quick issue resolution
- Improve observability


# 9. Monitoring System

Monitoring is handled using Airflow's built-in tools.

## Airflow UI Features
- DAG execution status
- Tasks success/failure tracking
- Execution time monitoring
- Logs for each task

## Monitoring Flow

```
Airflow Scheduler
       │
       ▼
Triggers DAG
       │
       ▼
Executes Tasks
       │
       ▼
Logs + Status Tracking
       │
       ▼
Alerts on Failure
```

---

# 10. Key Achievements 

This milestone successfully delivered:
- Automated pipeline scheduling 
- Integrated pipeline scheduling
- Fault-tolerant execution with retries
- Alert mechanism for failures 
- End-to-end monitoring using Airflow

---


