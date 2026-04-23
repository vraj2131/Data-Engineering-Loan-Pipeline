"""
Bronze Layer — PySpark Structured Streaming ingestion.

Reads CSV files from DATA_DIR, enforces schemas, and writes to the
PostgreSQL bronze schema via JDBC (append mode, exactly as the
original Databricks notebooks did with Delta tables).

Run via:  spark-submit --jars /opt/spark/jars/postgresql-42.7.3.jar bronze_ingestion.py
Or triggered by the Airflow DAG using BashOperator.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType, DateType
)

# ── Config ────────────────────────────────────────────────────────────────────
JDBC_URL   = os.getenv("JDBC_URL",   "jdbc:postgresql://localhost:5432/loandb")
DB_USER    = os.getenv("POSTGRES_USER",     "pipeline_user")
DB_PASS    = os.getenv("POSTGRES_PASSWORD", "pipeline_pass")
DATA_DIR   = os.getenv("DATA_DIR",   "/opt/airflow/data")
JDBC_JAR   = "/opt/spark/jars/postgresql-42.7.3.jar"

JDBC_PROPS = {
    "user":     DB_USER,
    "password": DB_PASS,
    "driver":   "org.postgresql.Driver",
}

# ── Spark session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("LoanPipeline_BronzeIngestion")
    .master(os.getenv("SPARK_MASTER", "local[*]"))
    .config("spark.jars", JDBC_JAR)
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Schema definitions ────────────────────────────────────────────────────────
SCHEMAS = {
    "loans": StructType([
        StructField("loan_id",               StringType(),    False),
        StructField("property_id",           StringType(),    True),
        StructField("borrower_id",           StringType(),    True),
        StructField("servicer_id",           StringType(),    True),
        StructField("origination_date",      DateType(),      True),
        StructField("maturity_date",         DateType(),      True),
        StructField("loan_amount",           DoubleType(),    True),
        StructField("interest_rate",         DoubleType(),    True),
        StructField("monthly_payment",       DoubleType(),    True),
        StructField("loan_purpose",          StringType(),    True),
        StructField("loan_status",           StringType(),    True),
        StructField("last_updated_timestamp", TimestampType(), True),
    ]),
    "borrowers": StructType([
        StructField("borrower_id",           StringType(),    False),
        StructField("first_name",            StringType(),    True),
        StructField("last_name",             StringType(),    True),
        StructField("credit_score",          IntegerType(),   True),
        StructField("annual_income",         DoubleType(),    True),
        StructField("dti_ratio",             DoubleType(),    True),
        StructField("employment_status",     StringType(),    True),
        StructField("years_employed",        IntegerType(),   True),
        StructField("last_updated_timestamp", TimestampType(), True),
    ]),
    "servicers": StructType([
        StructField("servicer_id",           StringType(),    False),
        StructField("servicer_name",         StringType(),    True),
        StructField("region",                StringType(),    True),
        StructField("state_code",            StringType(),    True),
        StructField("portfolio_size",        IntegerType(),   True),
        StructField("active_loans",          IntegerType(),   True),
        StructField("last_updated_timestamp", TimestampType(), True),
    ]),
    "properties": StructType([
        StructField("property_id",           StringType(),    False),
        StructField("state_code",            StringType(),    True),
        StructField("city",                  StringType(),    True),
        StructField("zip_code",              StringType(),    True),
        StructField("property_type",         StringType(),    True),
        StructField("appraised_value",       DoubleType(),    True),
        StructField("ltv_ratio",             DoubleType(),    True),
        StructField("year_built",            IntegerType(),   True),
        StructField("last_updated_timestamp", TimestampType(), True),
    ]),
    "loan_payments": StructType([
        StructField("payment_id",            StringType(),    False),
        StructField("loan_id",               StringType(),    True),
        StructField("payment_date",          DateType(),      True),
        StructField("scheduled_amount",      DoubleType(),    True),
        StructField("actual_amount",         DoubleType(),    True),
        StructField("remaining_balance",     DoubleType(),    True),
        StructField("days_past_due",         IntegerType(),   True),
        StructField("last_updated_timestamp", TimestampType(), True),
    ]),
    "states": StructType([
        StructField("state_id",              StringType(),    False),
        StructField("state_name",            StringType(),    True),
        StructField("state_code",            StringType(),    True),
        StructField("region",                StringType(),    True),
        StructField("median_household_income", DoubleType(), True),
        StructField("avg_home_price",        DoubleType(),    True),
        StructField("last_updated_timestamp", TimestampType(), True),
    ]),
}

# ── Ingest each table ─────────────────────────────────────────────────────────
for table_name, schema in SCHEMAS.items():
    csv_path = os.path.join(DATA_DIR, f"{table_name}.csv")
    print(f"[bronze] ingesting {csv_path} → bronze.{table_name}")

    df = (
        spark.read
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(schema)
        .csv(csv_path)
    )

    row_count = df.count()
    print(f"[bronze] {table_name}: {row_count} rows read from CSV")

    (
        df.write
        .mode("append")           # idempotent on re-run; dedup happens in silver
        .jdbc(JDBC_URL, f"bronze.{table_name}", properties=JDBC_PROPS)
    )
    print(f"[bronze] {table_name}: written to PostgreSQL bronze.{table_name}")

spark.stop()
print("[bronze] ingestion complete")
