{{
    config(
        materialized='incremental',
        unique_key='property_id'
    )
}}

{% set cols = [
    'property_id', 'state_code', 'city', 'zip_code',
    'property_type', 'appraised_value', 'ltv_ratio',
    'year_built', 'last_updated_timestamp'
] %}

SELECT
    {% for col in cols %}
        {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ source("source_bronze", "properties") }}

{% if is_incremental() %}
WHERE last_updated_timestamp > (
    SELECT COALESCE(MAX(last_updated_timestamp), '1900-01-01') FROM {{ this }}
)
{% endif %}
