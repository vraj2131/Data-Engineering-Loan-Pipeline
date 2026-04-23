{{
    config(
        materialized='incremental',
        unique_key='borrower_id'
    )
}}

{% set cols = [
    'borrower_id', 'first_name', 'last_name', 'credit_score',
    'annual_income', 'dti_ratio', 'employment_status',
    'years_employed', 'last_updated_timestamp'
] %}

SELECT
    {% for col in cols %}
        {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ source("source_bronze", "borrowers") }}

{% if is_incremental() %}
WHERE last_updated_timestamp > (
    SELECT COALESCE(MAX(last_updated_timestamp), '1900-01-01') FROM {{ this }}
)
{% endif %}
