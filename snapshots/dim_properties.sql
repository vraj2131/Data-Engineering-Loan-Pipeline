{% snapshot DimProperties %}
{{
    config(
        target_schema='gold',
        unique_key='property_id',
        strategy='timestamp',
        updated_at='last_updated_timestamp',
        invalidate_hard_deletes=True
    )
}}
SELECT * FROM {{ source('source_silver', 'properties') }}
{% endsnapshot %}
