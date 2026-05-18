-- Reinsurance: ceded premium and treaty utilization by reinsurer and treaty type.
-- Industry standard view for reinsurance and capital management teams.
-- Combines internal synthetic treaties with the migrated allianz_pc EUR treaties.

SELECT
    'USD_synthetic' AS source,
    treaty_type,
    reinsurer,
    line_of_business,
    ROUND(SUM(total_limit_usd)      / 1e6, 1) AS total_limit_musd,
    ROUND(SUM(premium_ceded_usd)    / 1e6, 1) AS ceded_premium_musd,
    ROUND(AVG(avg_cession_pct) * 100, 1)      AS avg_cession_pct,
    SUM(treaty_count)                         AS treaty_count
FROM serverless_stable_xhky6g_catalog.allianz_gold.reinsurance_summary
GROUP BY treaty_type, reinsurer, line_of_business

UNION ALL

SELECT
    'EUR_pc' AS source,
    treaty_type,
    reinsurer,
    line_of_business,
    ROUND(SUM(limit_eur_m), 1)                AS total_limit_musd,
    ROUND(SUM(ceded_premium_eur_m), 1)        AS ceded_premium_musd,
    ROUND(AVG(cession_pct), 1)                AS avg_cession_pct,
    COUNT(*)                                  AS treaty_count
FROM serverless_stable_xhky6g_catalog.allianz_silver.pc_reinsurance_treaties
GROUP BY treaty_type, reinsurer, line_of_business
ORDER BY ceded_premium_musd DESC
LIMIT 30;
