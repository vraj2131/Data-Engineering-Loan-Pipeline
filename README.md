# End-to-End Data Engineering Project — PySpark, Databricks & dbt

This repository contains an **end-to-end data engineering pipeline** that integrates **PySpark**, **Databricks**, and **dbt Cloud** to design a scalable medallion architecture (Bronze → Silver → Gold).  
The goal is to simulate **real-world analytics engineering and data transformation workflows**, delivering **business-ready KPIs** and **incremental, production-grade data pipelines**.

---

## 🧩 Project Overview

This project demonstrates how to build a **modern data engineering pipeline** that ingests raw streaming data, transforms it using PySpark and dbt, and exposes key business insights through a Gold layer.  
It showcases how to handle real-world challenges like incremental loading, deduplication, and change tracking while maintaining transparency, modularity, and scalability.

---

## 📰 Project Article  

**Medium Article:** [From Raw Chaos to Business Gold — Building an End-to-End Data Engineering Pipeline with PySpark](https://medium.com/@codegnerdev/from-raw-chaos-to-business-gold-building-an-end-to-end-data-engineering-pipeline-with-pyspark-cc6ca4e458ec)

---

### 🔗 Pipeline Overview

The full pipeline flow is as follows:
1. **Bronze Layer (Raw Ingestion):**  
   Data is streamed into the data lake (CSV files via PySpark Streaming) and stored as Delta tables.
2. **Silver Layer (Transformation & Cleansing):**  
   Cleaned and standardized data using dbt models (`trips.sql`), materialized incrementally with Jinja templating and logic.
3. **Gold Layer (Business KPIs):**  
   Aggregated tables with business metrics for decision-making, built with Jinja loops, conditional logic, and dbt snapshots.

---

## 🏗️ Bronze Layer — Data Ingestion

The **Bronze Layer** captures raw streaming data from CSV files using PySpark Streaming and stores them in a Databricks data lake as Delta tables.

Key highlights:
- Streaming ingestion using `readStream()`
- Schema enforcement for consistency
- Storage in `/mnt/bronze/` path for downstream use

---

## ⚙️ PySpark Streaming & Schema Definition

PySpark Streaming enables continuous ingestion and schema validation for the incoming trip data.

---

## 🧠 Silver Layer — Transformation (dbt + Jinja)

The **Silver Layer** focuses on cleansing, enriching, and incrementally updating the data.

We use **dbt Cloud** (Studio & CLI) to manage models and transformations, connecting directly to Databricks.

Example model:  
`models/silver/trips.sql`
```sql
{{ 
    config(
        materialized='incremental',
        unique_key='trip_id'
    )
}}

{% set cols = ['trip_id','vehicle_id','customer_id','driver_id',
'trip_start_time','trip_end_time','distance_km','fare_amount','last_updated_timestamp'] %}

SELECT 
    {% for col in cols %}
        {{ col }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ source("source_bronze", "trips") }}

{% if is_incremental() %}
WHERE last_updated_timestamp > (
    SELECT COALESCE(MAX(last_updated_timestamp), '1900-01-01') FROM {{ this }}
)
{% endif %}

````

---

This approach ensures:

* Incremental materialization (only new data processed)
* Source tracking via `source()` definitions
* Dynamic column selection via Jinja loops

---

## 🏅 Gold Layer — Business KPIs & Insights

The **Gold Layer** produces three key business views designed to simulate real-world analytical dashboards.

| View                      | Purpose                | Key Metrics                                       |
| ------------------------- | ---------------------- | ------------------------------------------------- |
| `trip_revenue_kpis`       | Financial overview     | Total fare, average fare, total distance          |
| `trip_driver_performance` | Workforce productivity | Total trips, revenue per km, performance category |
| `trip_customer_trends`    | Customer retention     | Average spend, loyalty segmentation               |

Each model uses **Jinja conditions and loops** for reusable logic and **dbt snapshots** to maintain historical changes.

---

## ⚡ Materialization & Snapshots

Materialization strategy in dbt ensures efficient data updates:

* **Bronze & Silver:** Incremental materialization using unique keys.
* **Gold:** Snapshot-based updates for time-travel and trend analysis.

---

## 🚀 Extension — Finance Domain Pipeline (Fully Local, Dockerized)

This extension re-domains the pipeline from ride-sharing to **mortgage loan analytics** (Freddie Mac-style data) and makes the entire stack runnable locally with Docker — no cloud account required.

### What Changed

| Component | Original | Extension |
|---|---|---|
| Data domain | Ride-sharing trips | Mortgage loans (loans, borrowers, servicers, properties, payments, states) |
| Warehouse | Databricks Delta Lake | PostgreSQL (Docker container) |
| Orchestration | Manual | Apache Airflow DAG (daily schedule) |
| Infrastructure | Cloud | Fully local via Docker Compose |

### Finance Domain — Gold Layer KPIs

| Model | Purpose | Key Metrics |
|---|---|---|
| `loan_risk_kpis` | Monthly portfolio risk overview | Total originations, delinquency rate, risk tier (High/Medium/Low) |
| `servicer_performance` | Per-servicer portfolio health | Delinquency rate, servicer health rating (Excellent/Good/At Risk/Critical) |
| `borrower_risk_profile` | Per-borrower credit analysis | Credit tier (Prime/Near-Prime/Subprime), DTI risk band, underwriting category |

SCD Type 2 snapshots track historical changes for all 5 dimension tables (Borrowers, Servicers, Properties, States, LoanPayments) and the FactLoans table.

### Airflow DAG — `loan_pipeline`

Runs daily at 06:00 UTC with 5 sequential tasks:

```
start → bronze_ingestion → dbt_silver_run → dbt_gold_run → dbt_snapshots_run → dbt_test → end
```

- **bronze_ingestion** — PySpark reads 6 CSV files, enforces typed schemas, writes to PostgreSQL `bronze` schema via JDBC
- **dbt_silver_run** — Incremental dbt models deduplicate and promote data to `silver` schema
- **dbt_gold_run** — Builds 3 analytical gold tables
- **dbt_snapshots_run** — SCD Type 2 snapshot updates for all dims and fact
- **dbt_test** — dbt data quality tests on gold schema

**Successful pipeline run — all 7 tasks green:**

![Airflow Pipeline Run](Images/Screenshot%202026-04-23%20at%205.18.13%20PM.png)

### Running Locally

**Prerequisites:** Docker Desktop installed and running.

**1. Generate Airflow secret keys**
```bash
python3 -c "import secrets, base64; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
```
Paste the output into `.env` for `AIRFLOW__CORE__FERNET_KEY` and `AIRFLOW__WEBSERVER__SECRET_KEY`.

**2. Start all services**
```bash
docker compose up --build -d
docker compose logs -f airflow-init
# Wait for: "User admin created with role Admin"
```

**3. Open Airflow UI**

Go to **http://localhost:8080** — login: `admin` / `admin`

Trigger the `loan_pipeline` DAG with the ▶ button.

**4. Inspect results**
```bash
docker exec -it $(docker compose ps -q postgres) psql -U pipeline_user -d loandb
```
```sql
SELECT * FROM gold.loan_risk_kpis;
SELECT * FROM gold.servicer_performance;
SELECT * FROM gold.borrower_risk_profile LIMIT 10;
```

**5. Teardown**
```bash
docker compose down      # stop, keep data
docker compose down -v   # stop + wipe everything
```

### Using Databricks Instead of Local PostgreSQL

This extension runs entirely on PostgreSQL in Docker for portability. To replicate the original Databricks-backed architecture with this finance domain:

**Bronze Layer**  
Replace `scripts/bronze_ingestion.py` with a Databricks notebook using the same PySpark schema definitions. Write to Delta tables instead of JDBC:
```python
df.write.format("delta").mode("append").save("/mnt/bronze/loans")
```

**Silver & Gold (dbt)**  
In `profiles.yml`, swap the `type: postgres` output for `type: databricks` using the [dbt-databricks adapter](https://docs.getdbt.com/docs/core/connect-data-platform/databricks-setup):
```yaml
dev:
  type: databricks
  host: <your-workspace>.azuredatabricks.net
  http_path: /sql/1.0/warehouses/<warehouse-id>
  token: "{{ env_var('DBT_TOKEN') }}"
  schema: silver
```
All dbt models, macros, and snapshots work unchanged — only the connection profile changes.

**Airflow**  
Replace the `BashOperator` bronze task with Databricks' native operator:
```python
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

bronze_ingestion = DatabricksRunNowOperator(
    task_id="bronze_ingestion",
    databricks_conn_id="databricks_default",
    job_id="<your-job-id>",
)
```
Install the provider: `pip install apache-airflow-providers-databricks`

The dbt silver/gold/snapshot tasks remain identical `BashOperator` calls — dbt handles the Databricks connection through the profile.

---

## 🌍 Real-World Impact & Learnings

### 🔧 Real-World Problem Solving

This pipeline reflects real-world data engineering solutions:

* **Reliable data lineage:** dbt sources and lineage graphs provide traceability.
* **Incremental & cost-efficient:** Only new data loads—ideal for large streaming systems.
* **Business-ready insights:** KPIs in the Gold layer empower finance, ops, and product teams.
* **Governance & testing:** Built-in dbt tests and documentation enhance data trust.
* **Scalable design:** Follows Databricks Medallion Architecture—extendable to enterprise scale.

### 🎓 What I Learned

Building this project has helped me:

* Master **dbt incremental models**, **snapshots**, and **Jinja templating**.
* Understand **Databricks Delta Lake** and **PySpark Streaming** for structured streaming.
* Learn **data modeling, testing, and documentation** in dbt Cloud.
* Apply real-world engineering practices like **source control**, **versioning**, and **environment management**.
* Orchestrate multi-step pipelines with **Apache Airflow** and containerize full stacks with **Docker Compose**.
* Apply finance-domain data modeling — credit tiers, delinquency rates, SCD Type 2 history tracking.

### 💼 Why it Matters

This project mirrors how modern companies manage their data ecosystems—turning raw events into actionable insights with reproducible, testable, and efficient pipelines.
It demonstrates my ability to design, document, and operationalize **end-to-end data engineering workflows** using **industry-grade tools**.

---

## 🧱 Project Structure

```
pyspark-dbt-project/
│
├── dags/
│   └── loan_pipeline_dag.py       # Airflow DAG — full pipeline orchestration
│
├── models/
│   ├── silver/
│   │   ├── loans.sql
│   │   ├── borrowers.sql
│   │   ├── servicers.sql
│   │   ├── properties.sql
│   │   ├── loan_payments.sql
│   │   └── states.sql
│   ├── gold/
│   │   ├── loan_risk_kpis.sql
│   │   ├── servicer_performance.sql
│   │   └── borrower_risk_profile.sql
│   └── sources/
│       └── sources.yaml
│
├── snapshots/
│   ├── fact_loans.sql
│   ├── dim_borrowers.sql
│   ├── dim_servicers.sql
│   ├── dim_properties.sql
│   ├── dim_states.sql
│   └── dim_loan_payments.sql
│
├── macros/
│   ├── kpi_helpers.sql            # portfolio_risk_tier() macro
│   └── generate_schema_name.sql
│
├── scripts/
│   ├── bronze_ingestion.py        # PySpark ingestion script
│   └── init_db.sql                # PostgreSQL schema bootstrap
│
├── data/
│   ├── loans.csv
│   ├── borrowers.csv
│   ├── servicers.csv
│   ├── properties.csv
│   ├── loan_payments.csv
│   └── states.csv
│
├── Dockerfile
├── docker-compose.yml
├── profiles.yml                   # dbt connection config (reads from .env)
├── dbt_project.yml
├── requirements.txt
├── .env                           # secrets — not committed to git
└── README.md
```

---

## 🧠 Key Takeaways

* ✅ **End-to-End coverage:** Raw ingestion → Transformation → KPIs
* 🧰 **Tools used:** PySpark, dbt, Airflow, PostgreSQL, Docker, Delta Lake
* 🔄 **Incremental processing:** Upsert logic with unique keys
* 📊 **Real insights:** Loan risk, servicer health, and borrower credit metrics
* 📈 **Enterprise-ready:** Scalable medallion design with modular dbt models
* 🐳 **Fully portable:** Entire stack runs locally with a single `docker compose up`

---
