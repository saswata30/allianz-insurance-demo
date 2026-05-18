-- Real-time event risk dashboard query: every active NOAA / catastrophe event
-- with the policy exposure it touches. Used by event response teams.

SELECT
    e.event_type,
    e.severity,
    e.urgency,
    e.state_code,
    e.state_name,
    e.effective_utc,
    e.expires_utc,
    ROUND(e.exposed_tiv_usd / 1e9, 2)         AS exposed_tiv_busd,
    e.exposed_policy_count
FROM serverless_stable_xhky6g_catalog.allianz_gold.event_risk_correlation e
WHERE e.exposed_tiv_usd IS NOT NULL
ORDER BY e.exposed_tiv_usd DESC
LIMIT 50;
