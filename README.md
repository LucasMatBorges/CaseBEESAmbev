# 📘 BEES Data Engineering – Breweries Case  
### *End-to-End Cloud Data Pipeline (Airflow → ADF → Databricks → SQL)*

This repository contains a complete **end-to-end data engineering solution** developed for the *BEES Data Engineering – Breweries Case*.  
The project demonstrates ingestion, orchestration, monitoring, transformation, testing, and publishing of data using best practices and modern data engineering standards.

---

## 🏗️ 1. Project Overview

This solution implements the full lifecycle of a data pipeline:

- API ingestion from **Open Brewery DB**
- Storage following the **Medallion Architecture** (Bronze → Silver → Gold)
- Distributed processing using **Databricks** and **PySpark**
- Orchestration using **Airflow**
- Monitoring and triggering using **Azure Data Factory (ADF)**
- Automated unit testing of business logic
- Publishing final Gold tables to SQL
- Integration with **GitHub** for version control
- Containerized orchestration environment (Airflow in Docker)

The objective is to demonstrate an end-to-end pipeline from data ingestion to client-ready analytical tables.

---

## 🧱 2. System Architecture
1. **Open Brewery API**
2. **Airflow** – Scheduling, retries, logs
3. **Azure Data Factory** – Monitoring + triggers Databricks
4. **Databricks** – Bronze → Silver → Gold
5. **SQL / Delta Tables** – Final client-facing layer
---
## 🧰 3. Technology Stack

| Component | Purpose |
|----------|---------|
| **Airflow (Docker)** | Pipeline orchestration, scheduling, retries |
| **Azure Data Factory** | High-level monitoring, failure alerts, job triggers |
| **Databricks** | Distributed processing, Medallion Architecture |
| **PySpark** | Transformations and ETL logic |
| **Python** | API ingestion + utility functions |
| **Delta/Parquet** | Persistent storage format |
| **GitHub** | Version control & integration |
| **SQL** | Final consumption layer |

---

## 🥇 4. Medallion Architecture

### 🥉 Bronze Layer – Raw JSON Ingestion
- Airflow fetches the raw API data.
- Stored in **JSON Lines**, preserving the API schema.
- Includes ingestion partition by date.

**Example path:**
/bronze/breweries/date=YYYY-MM-DD/raw.json


### 🥈 Silver Layer – Standardized Data
- Implemented in Databricks using PySpark.
- Cleaning steps include:
  - Trimming text fields  
  - Casting lat/long to numeric  
  - Preserving all input fields  
  - Adding metadata: `ingest_date`, `ingest_timestamp`
- Stored as **Delta/Parquet**
- Partitioned by **country/state**

### 🥇 Gold Layer – Aggregated Analytics
- Aggregates brewery counts by:
  - country  
  - state  
  - brewery_type  
- Produces:
  - `brewery_count`
  - `last_ingest_date`
- Published as an analytical SQL table.

---

## 🧩 5. Modular Functions & Unit Testing

All transformation logic is implemented as pure functions inside `src/`:
src/
silver_logic.py
gold_logic.py


This design ensures:
- Clean separation of logic
- Easy testing
- Reusable transformations

### ✔ Unit Tests
Located in `tests/`, executed inside Databricks:
tests/
test_silver_logic
test_gold_logic


Tests validate:
- Field trimming  
- Type casting  
- Aggregation accuracy  
- Ingestion metadata  
- Partitioning logic  

---

## ⏱ 6. Orchestration

### **Airflow**
- Containerized using Docker
- Manages ingestion scheduling
- Handles retries and failure logic
- Triggers ADF after successful ingestion

### **Azure Data Factory**
- Monitors pipeline runs
- Provides enterprise-level alerting
- Triggers Databricks notebook jobs

### **Databricks**
- Runs Bronze → Silver → Gold processing
- Loads transformation code from `src/`
- Writes final datasets to Delta Lake

---

## 🔍 7. Monitoring & Alerting

### Airflow
- Retry logic  
- Task logs  
- XCom metadata  
- Email alerts (configurable)

### ADF
- Monitoring dashboard  
- Alerts for pipeline failures  
- Trigger history logging  

### Databricks
- Job logs  
- Data quality validation using Delta expectations  

---

## 🔗 8. GitHub Integration

GitHub stores:
- Airflow DAGs  
- Databricks notebooks  
- Transformation functions  
- Unit tests  
- Docker files  

A Personal Access Token (PAT) is used for:
repo, workflow, read:user


---

## 🗄️ 9. SQL Output Layer

Gold table is published for final consumption:

```sql
SELECT *
FROM gold_breweries
ORDER BY country, state, brewery_type;
```
---
▶️ 10. How to Run
1. **Start Airflow**   ```docker-compose up --build```

2. **Trigger ADF Pipeline**   Through ADF UI or REST API.

3. **Databricks Executes Transformations**  Runs Bronze → Silver → Gold notebooks.

4. **Run Tests**   Navigate to /tests in Databricks and run each notebook.



5. **Query the Gold Table**
  Use SQL:
```SELECT * FROM bees.brewery_gold;```



