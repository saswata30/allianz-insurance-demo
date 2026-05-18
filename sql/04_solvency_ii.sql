-- Solvency II reporting: SCR composition, own funds, solvency ratio.
-- Regulatory thresholds:
--   Solvency Ratio < 100%  → Capital intervention
--   100% to 150%           → Recovery plan required
--   > 150%                 → Healthy (Allianz Group targets ~200%)
-- Uses pc_solvency_metrics (EUR-denominated regulatory series).

SELECT
    reporting_date,
    ROUND(total_scr_eur_m, 1)                        AS total_scr_eur_m,
    ROUND(scr_market_risk, 1)                        AS market_risk,
    ROUND(scr_underwriting_non_life, 1)              AS uw_non_life,
    ROUND(scr_underwriting_life, 1)                  AS uw_life,
    ROUND(scr_underwriting_health, 1)                AS uw_health,
    ROUND(scr_credit_risk, 1)                        AS credit_risk,
    ROUND(scr_operational_risk, 1)                   AS operational_risk,
    ROUND(diversification_benefit, 1)                AS diversification_benefit,
    ROUND(own_funds_eur_m, 1)                        AS own_funds_eur_m,
    ROUND(mcr_eur_m, 1)                              AS mcr_eur_m,
    ROUND(solvency_ratio_pct, 1)                     AS solvency_ratio_pct,
    CASE
        WHEN solvency_ratio_pct >= 150 THEN 'Healthy'
        WHEN solvency_ratio_pct >= 100 THEN 'Recovery plan'
        ELSE 'Intervention'
    END                                              AS regulatory_status
FROM serverless_stable_xhky6g_catalog.allianz_silver.pc_solvency_metrics
ORDER BY reporting_date DESC;
