{{
    config(materialized='table')
}}

SELECT
    l.servicer_id,
    s.servicer_name,
    s.region,
    COUNT(l.loan_id)                                             AS total_loans,
    SUM(l.loan_amount)                                           AS total_portfolio_value,
    ROUND(AVG(l.loan_amount), 2)                                 AS avg_loan_amount,
    ROUND(AVG(l.interest_rate), 4)                               AS avg_interest_rate,
    COUNT(CASE WHEN l.loan_status != 'Current' THEN 1 END)       AS delinquent_count,
    ROUND(
        COUNT(CASE WHEN l.loan_status != 'Current' THEN 1 END)::NUMERIC
        / NULLIF(COUNT(l.loan_id), 0) * 100,
        2
    )                                                            AS delinquency_rate_pct,
    CASE
        WHEN COUNT(CASE WHEN l.loan_status != 'Current' THEN 1 END)::NUMERIC
             / NULLIF(COUNT(l.loan_id), 0) < 0.05  THEN 'Excellent'
        WHEN COUNT(CASE WHEN l.loan_status != 'Current' THEN 1 END)::NUMERIC
             / NULLIF(COUNT(l.loan_id), 0) < 0.10  THEN 'Good'
        WHEN COUNT(CASE WHEN l.loan_status != 'Current' THEN 1 END)::NUMERIC
             / NULLIF(COUNT(l.loan_id), 0) < 0.20  THEN 'At Risk'
        ELSE 'Critical'
    END                                                          AS servicer_health
FROM {{ ref('loans') }} l
LEFT JOIN {{ source('source_bronze', 'servicers') }} s
    ON l.servicer_id = s.servicer_id
GROUP BY l.servicer_id, s.servicer_name, s.region
