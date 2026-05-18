-- Industry-standard P&C underwriting KPIs by line of business and month.
-- Benchmarks (US P&C industry, AM Best 2024):
--   Personal Auto:        ~104%
--   Homeowners:           ~110%
--   Commercial Property:  ~95%
--   Workers Comp:         ~85%
--   Industry overall:    ~101%
-- A combined ratio < 100 indicates an underwriting profit.

SELECT
    DATE_TRUNC('MONTH', month)                                  AS period_month,
    line_of_business,
    ROUND(SUM(gross_written_premium_usd) / 1e6, 2)              AS gwp_musd,
    ROUND(SUM(net_written_premium_usd)   / 1e6, 2)              AS nwp_musd,
    ROUND(SUM(incurred_loss_usd)         / 1e6, 2)              AS incurred_loss_musd,
    SUM(claim_count)                                            AS claims,
    ROUND(AVG(loss_ratio) * 100, 1)                             AS loss_ratio_pct,
    ROUND(AVG(expense_ratio) * 100, 1)                          AS expense_ratio_pct,
    ROUND(AVG(combined_ratio) * 100, 1)                         AS combined_ratio_pct
FROM serverless_stable_xhky6g_catalog.allianz_gold.underwriting_kpis
GROUP BY DATE_TRUNC('MONTH', month), line_of_business
ORDER BY period_month DESC, line_of_business;
