-- Claim 360: a single row per claim joined to the policy, customer, geography.
-- Standard P&C claims operations view used by adjusters and claims leadership.

SELECT
    c.claim_id,
    c.line_of_business,
    c.product,
    c.peril,
    c.claim_status,
    c.loss_date,
    c.report_date,
    c.report_lag_days,
    ROUND(c.incurred_amount_usd, 0)       AS incurred_usd,
    ROUND(c.paid_amount_usd, 0)           AS paid_usd,
    ROUND(c.reserve_amount_usd, 0)        AS reserve_usd,
    ROUND(c.severity_pct_of_sum_insured * 100, 1)
                                          AS severity_pct_of_si,
    c.is_cat,
    c.fraud_flag,
    c.catastrophe_code,
    c.customer_name,
    c.customer_type,
    c.loyalty_tier,
    c.state_code,
    c.region,
    c.cresta_zone,
    c.flood_zone,
    c.channel
FROM serverless_stable_xhky6g_catalog.allianz_gold.claim_360 c
WHERE c.report_date >= add_months(current_date(), -12)
ORDER BY c.loss_date DESC
LIMIT 100;
