{{
    config(
        materialized='incremental',
        unique_key='payment_id'
    )
}}

{% set cols = [
    'payment_id', 'loan_id', 'payment_date', 'scheduled_amount',
    'actual_amount', 'remaining_balance', 'days_past_due',
    'last_updated_timestamp'
] %}

SELECT
    {% for col in cols %}
        {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ source("source_bronze", "loan_payments") }}

{% if is_incremental() %}
WHERE last_updated_timestamp > (
    SELECT COALESCE(MAX(last_updated_timestamp), '1900-01-01') FROM {{ this }}
)
{% endif %}
