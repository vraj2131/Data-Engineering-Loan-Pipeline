{% snapshot DimLoanPayments %}
{{
    config(
        target_schema='gold',
        unique_key='payment_id',
        strategy='timestamp',
        updated_at='last_updated_timestamp',
        invalidate_hard_deletes=True
    )
}}
SELECT * FROM {{ source('source_silver', 'loan_payments') }}
{% endsnapshot %}
