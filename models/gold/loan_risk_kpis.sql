{{
    config(materialized='table')
}}

{% set metrics = [
    {'name': 'total_loans',         'expr': 'COUNT(loan_id)'},
    {'name': 'total_origination',   'expr': 'SUM(loan_amount)'},
    {'name': 'avg_loan_amount',     'expr': 'AVG(loan_amount)'},
    {'name': 'avg_interest_rate',   'expr': 'ROUND(AVG(interest_rate), 4)'},
    {'name': 'delinquent_loans',    'expr': "COUNT(CASE WHEN loan_status != 'Current' THEN 1 END)"},
    {'name': 'foreclosure_count',   'expr': "COUNT(CASE WHEN loan_status = 'Foreclosure' THEN 1 END)"}
] %}

SELECT
    DATE_TRUNC('month', origination_date) AS origination_month,
    loan_purpose,
    {% for m in metrics %}
        {{ m.expr }} AS {{ m.name }}{% if not loop.last %},{% endif %}
    {% endfor %},
    ROUND(
        COUNT(CASE WHEN loan_status != 'Current' THEN 1 END)::NUMERIC
        / NULLIF(COUNT(loan_id), 0) * 100,
        2
    ) AS delinquency_rate_pct,
    {{ portfolio_risk_tier('AVG(interest_rate)') }} AS risk_tier
FROM {{ ref('loans') }}
GROUP BY 1, 2
