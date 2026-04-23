{% macro portfolio_risk_tier(avg_rate_expr) %}
    CASE
        WHEN {{ avg_rate_expr }} >= 6.0 THEN 'High Risk'
        WHEN {{ avg_rate_expr }} >= 4.0 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END
{% endmacro %}
