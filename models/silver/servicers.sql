{{
    config(
        materialized='incremental',
        unique_key='servicer_id'
    )
}}

{% set cols = [
    'servicer_id', 'servicer_name', 'region', 'state_code',
    'portfolio_size', 'active_loans', 'last_updated_timestamp'
] %}

SELECT
    {% for col in cols %}
        {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ source("source_bronze", "servicers") }}

{% if is_incremental() %}
WHERE last_updated_timestamp > (
    SELECT COALESCE(MAX(last_updated_timestamp), '1900-01-01') FROM {{ this }}
)
{% endif %}
