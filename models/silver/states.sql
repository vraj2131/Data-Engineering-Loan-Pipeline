{{
    config(
        materialized='incremental',
        unique_key='state_id'
    )
}}

{% set cols = [
    'state_id', 'state_name', 'state_code', 'region',
    'median_household_income', 'avg_home_price', 'last_updated_timestamp'
] %}

SELECT
    {% for col in cols %}
        {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ source("source_bronze", "states") }}

{% if is_incremental() %}
WHERE last_updated_timestamp > (
    SELECT COALESCE(MAX(last_updated_timestamp), '1900-01-01') FROM {{ this }}
)
{% endif %}
