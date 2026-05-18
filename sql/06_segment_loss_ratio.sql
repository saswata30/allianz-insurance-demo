-- Segment loss ratio: identify under-performing segments at the intersection of
-- LOB × Product × State × Channel × Customer Type. Standard P&C portfolio
-- profitability drill-down. Sort to find segments needing rate action.

SELECT
    line_of_business,
    product,
    state_code,
    channel,
    customer_type,
    ROUND(gross_premium_usd  / 1e6, 2)   AS gwp_musd,
    ROUND(incurred_loss_usd  / 1e6, 2)   AS losses_musd,
    claim_count,
    ROUND(loss_ratio * 100, 1)            AS loss_ratio_pct,
    ROUND(expense_ratio * 100, 1)         AS expense_ratio_pct,
    ROUND(combined_ratio * 100, 1)        AS combined_ratio_pct
FROM serverless_stable_xhky6g_catalog.allianz_gold.loss_ratio_by_segment
WHERE gross_premium_usd > 50000           -- ignore micro-segments
ORDER BY combined_ratio_pct DESC
LIMIT 30;
