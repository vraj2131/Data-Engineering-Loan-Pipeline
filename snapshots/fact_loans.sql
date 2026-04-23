{% snapshot FactLoans %}
{{
    config(
        target_schema='gold',
        unique_key='loan_id',
        strategy='timestamp',
        updated_at='last_updated_timestamp',
        invalidate_hard_deletes=True
    )
}}
SELECT * FROM {{ ref('loans') }}
{% endsnapshot %}
