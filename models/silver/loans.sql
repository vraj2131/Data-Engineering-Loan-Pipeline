{{
    config(
        materialized='incremental',
        unique_key='loan_id'
    )
}}

{% set cols = [
    'loan_id', 'property_id', 'borrower_id', 'servicer_id',
    'origination_date', 'maturity_date',
    'loan_amount', 'interest_rate', 'monthly_payment',
    'loan_purpose', 'loan_status', 'last_updated_timestamp'
] %}

SELECT
    {% for col in cols %}
        {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ source("source_bronze", "loans") }}

{% if is_incremental() %}
WHERE last_updated_timestamp > (
    SELECT COALESCE(MAX(last_updated_timestamp), '1900-01-01') FROM {{ this }}
)
{% endif %}
