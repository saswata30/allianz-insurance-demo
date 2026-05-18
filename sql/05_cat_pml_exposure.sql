-- Catastrophe PML exposure: live external catastrophe events × policy exposure by state.
-- Joins gold.cat_event_pml with internal exposure to produce an actionable risk view.

SELECT
    c.event_type,
    c.severity,
    c.title,
    c.state_code,
    g.state_name,
    g.region,
    ROUND(c.state_tiv_usd / 1e9, 2)         AS state_tiv_busd,
    ROUND(c.severity_factor * 100, 1)       AS severity_factor_pct,
    ROUND(c.pml_estimate_usd / 1e6, 1)      AS pml_musd,
    c.event_time_utc
FROM       serverless_stable_xhky6g_catalog.allianz_gold.cat_event_pml c
LEFT JOIN  (SELECT state_code, MAX(state_name) AS state_name, MAX(region) AS region
            FROM serverless_stable_xhky6g_catalog.allianz_silver.dim_geography
            GROUP BY state_code) g
       ON  c.state_code = g.state_code
WHERE c.pml_estimate_usd IS NOT NULL
ORDER BY c.pml_estimate_usd DESC
LIMIT 50;
