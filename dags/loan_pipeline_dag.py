"""
Loan Pipeline DAG
─────────────────
Schedule: daily at 06:00 UTC

Flow:
  1. bronze_ingestion  — PySpark reads CSVs → PostgreSQL bronze schema
  2. dbt_silver_run    — dbt runs the incremental loans silver model
  3. dbt_gold_run      — dbt builds all three gold tables
  4. dbt_snapshots_run — dbt snapshots (SCD Type 2) for all dims + fact
  5. dbt_test          — dbt tests on gold schema
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ── Shared config ─────────────────────────────────────────────────────────────
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"

PATH_EXPORT = "export PATH=$PATH:/home/airflow/.local/bin"
DBT_FLAGS = f"--profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}"

SPARK_SUBMIT = (
    "export PATH=$PATH:/home/airflow/.local/bin && "
    "spark-submit "
    "--master local[*] "
    "--jars /opt/spark/jars/postgresql-42.7.3.jar "
    "/opt/airflow/scripts/bronze_ingestion.py"
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="loan_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end loan data pipeline: Bronze → Silver → Gold → Snapshots",
    schedule="0 6 * * *",         # daily at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finance", "pyspark", "dbt", "medallion"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ── Step 1: Bronze ingestion (PySpark) ────────────────────────────────────
    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=SPARK_SUBMIT,
        env={
            "JAVA_HOME":        "/usr/lib/jvm/java-17-openjdk",
            "SPARK_MASTER":     "local[*]",
            "DATA_DIR":         "/opt/airflow/data",
            "JDBC_URL":         "jdbc:postgresql://postgres:5432/loandb",
            "POSTGRES_USER":    "pipeline_user",
            "POSTGRES_PASSWORD":"pipeline_pass",
        },
        append_env=True,
    )

    # ── Step 2: dbt silver ────────────────────────────────────────────────────
    dbt_silver = BashOperator(
        task_id="dbt_silver_run",
        bash_command=f"{PATH_EXPORT} && dbt run --select silver {DBT_FLAGS}",
    )

    # ── Step 3: dbt gold ──────────────────────────────────────────────────────
    dbt_gold = BashOperator(
        task_id="dbt_gold_run",
        bash_command=f"{PATH_EXPORT} && dbt run --select gold {DBT_FLAGS}",
    )

    # ── Step 4: dbt snapshots ─────────────────────────────────────────────────
    dbt_snapshots = BashOperator(
        task_id="dbt_snapshots_run",
        bash_command=f"{PATH_EXPORT} && dbt snapshot {DBT_FLAGS}",
    )

    # ── Step 5: dbt test ──────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{PATH_EXPORT} && dbt test --select gold {DBT_FLAGS}",
    )

    # ── Dependency chain ──────────────────────────────────────────────────────
    start >> bronze_ingestion >> dbt_silver >> dbt_gold >> dbt_snapshots >> dbt_test >> end
