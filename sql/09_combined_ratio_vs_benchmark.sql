-- Combined ratio vs industry benchmarks per sub-line.
-- Uses the migrated pc_combined_ratios quarterly series.

WITH benchmarks AS (
    SELECT 'Auto'        AS sub_line, 104.0 AS industry_combined_ratio_pct
    UNION ALL SELECT 'Home',          110.0
    UNION ALL SELECT 'Liability',      95.0
    UNION ALL SELECT 'Property',       95.0
    UNION ALL SELECT 'Marine',         92.0
    UNION ALL SELECT 'Aviation',       98.0
    UNION ALL SELECT 'Cyber',         105.0
    UNION ALL SELECT 'WorkersComp',    85.0
)
SELECT
    cr.reporting_quarter,
    cr.line_of_business,
    cr.sub_line,
    cr.loss_ratio_pct,
    cr.expense_ratio_pct,
    cr.combined_ratio_pct,
    b.industry_combined_ratio_pct        AS benchmark_pct,
    ROUND(cr.combined_ratio_pct - COALESCE(b.industry_combined_ratio_pct, 100), 1)
                                         AS delta_vs_benchmark_pts,
    CASE
        WHEN cr.combined_ratio_pct < 100                                          THEN 'Profitable'
        WHEN cr.combined_ratio_pct < COALESCE(b.industry_combined_ratio_pct, 100) THEN 'Worse than market — review'
        ELSE 'Worse than market and unprofitable — rate action'
    END AS underwriting_signal
FROM serverless_stable_xhky6g_catalog.allianz_silver.pc_combined_ratios cr
LEFT JOIN benchmarks b ON cr.sub_line = b.sub_line
ORDER BY cr.reporting_quarter DESC, cr.combined_ratio_pct DESC;
