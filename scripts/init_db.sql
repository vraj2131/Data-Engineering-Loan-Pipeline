-- Runs automatically when the postgres container first starts.
-- Creates the Airflow metadata DB and the three pipeline schemas.

-- Airflow metadata database
CREATE DATABASE airflow_metadata;

-- Pipeline schemas inside loandb (loandb is already created by POSTGRES_DB env var)
\connect loandb

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

GRANT ALL PRIVILEGES ON SCHEMA bronze TO pipeline_user;
GRANT ALL PRIVILEGES ON SCHEMA silver TO pipeline_user;
GRANT ALL PRIVILEGES ON SCHEMA gold   TO pipeline_user;

-- Pre-create bronze tables so PySpark JDBC writes don't need DDL permissions.
-- PySpark will overwrite these with .mode("append") on first run.

CREATE TABLE IF NOT EXISTS bronze.loans (
    loan_id               VARCHAR(20),
    property_id           VARCHAR(20),
    borrower_id           VARCHAR(20),
    servicer_id           VARCHAR(10),
    origination_date      DATE,
    maturity_date         DATE,
    loan_amount           NUMERIC(14,2),
    interest_rate         NUMERIC(5,2),
    monthly_payment       NUMERIC(10,2),
    loan_purpose          VARCHAR(50),
    loan_status           VARCHAR(50),
    last_updated_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.borrowers (
    borrower_id            VARCHAR(20),
    first_name             VARCHAR(100),
    last_name              VARCHAR(100),
    credit_score           INT,
    annual_income          NUMERIC(12,2),
    dti_ratio              NUMERIC(5,2),
    employment_status      VARCHAR(50),
    years_employed         INT,
    last_updated_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.servicers (
    servicer_id            VARCHAR(10),
    servicer_name          VARCHAR(200),
    region                 VARCHAR(50),
    state_code             VARCHAR(5),
    portfolio_size         INT,
    active_loans           INT,
    last_updated_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.properties (
    property_id            VARCHAR(20),
    state_code             VARCHAR(5),
    city                   VARCHAR(100),
    zip_code               VARCHAR(10),
    property_type          VARCHAR(50),
    appraised_value        NUMERIC(14,2),
    ltv_ratio              NUMERIC(5,2),
    year_built             INT,
    last_updated_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.loan_payments (
    payment_id             VARCHAR(20),
    loan_id                VARCHAR(20),
    payment_date           DATE,
    scheduled_amount       NUMERIC(10,2),
    actual_amount          NUMERIC(10,2),
    remaining_balance      NUMERIC(14,2),
    days_past_due          INT,
    last_updated_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.states (
    state_id               VARCHAR(10),
    state_name             VARCHAR(100),
    state_code             VARCHAR(5),
    region                 VARCHAR(50),
    median_household_income NUMERIC(12,2),
    avg_home_price         NUMERIC(14,2),
    last_updated_timestamp TIMESTAMP
);
