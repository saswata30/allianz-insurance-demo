-- Portfolio book health: frequency (claims per 100 policies) and severity by LOB & product.
-- Industry benchmarks (US P&C, ISO/NAIC 2024):
--   Personal Auto:  freq ~6 /100, severity ~$5K
--   Homeowners:     freq ~5 /100, severity ~$15K
--   Commercial Property: freq ~3 /100, severity ~$60K
--   Cyber: freq ~1.5 /100, severity ~$200K (rising)

SELECT
    line_of_business,
    product,
    active_policy_count,
    ROUND(inforce_premium_usd  / 1e6, 2) AS inforce_gwp_musd,
    ROUND(avg_premium_per_policy, 0)     AS avg_premium_usd,
    ROUND(claim_frequency_per_100, 2)    AS freq_per_100_policies,
    ROUND(avg_severity_usd, 0)           AS avg_severity_usd,
    ROUND(loss_ratio * 100, 1)           AS loss_ratio_pct,
    cat_claim_count,
    fraud_claim_count,
    ROUND(avg_underwriter_score, 1)      AS avg_underwriter_score
FROM serverless_stable_xhky6g_catalog.allianz_gold.book_health
ORDER BY inforce_gwp_musd DESC;
