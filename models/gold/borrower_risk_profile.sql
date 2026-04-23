{{
    config(materialized='table')
}}

SELECT
    b.borrower_id,
    b.first_name || ' ' || b.last_name   AS borrower_name,
    b.credit_score,
    b.annual_income,
    b.dti_ratio,
    b.employment_status,
    COUNT(l.loan_id)                      AS total_loans,
    SUM(l.loan_amount)                    AS total_exposure,
    ROUND(AVG(l.interest_rate), 4)        AS avg_interest_rate,
    MAX(l.loan_status)                    AS worst_loan_status,
    CASE
        WHEN b.credit_score >= 740 THEN 'Prime'
        WHEN b.credit_score >= 670 THEN 'Near-Prime'
        WHEN b.credit_score >= 580 THEN 'Subprime'
        ELSE 'Deep-Subprime'
    END                                   AS credit_tier,
    CASE
        WHEN b.dti_ratio < 36 THEN 'Low Risk'
        WHEN b.dti_ratio < 43 THEN 'Moderate Risk'
        ELSE 'High Risk'
    END                                   AS dti_risk_band,
    CASE
        WHEN b.credit_score >= 740 AND b.dti_ratio < 36 THEN 'Preferred'
        WHEN b.credit_score >= 670 AND b.dti_ratio < 43 THEN 'Standard'
        ELSE 'Elevated'
    END                                   AS underwriting_category
FROM {{ source('source_bronze', 'borrowers') }} b
LEFT JOIN {{ ref('loans') }} l
    ON b.borrower_id = l.borrower_id
GROUP BY
    b.borrower_id, b.first_name, b.last_name,
    b.credit_score, b.annual_income, b.dti_ratio, b.employment_status
