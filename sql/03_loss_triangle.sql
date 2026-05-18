-- Loss triangle (loss development) by accident year and development period.
-- Uses the pc_loss_development reference table migrated from allianz_pc.
-- Industry standard: actuaries use loss triangles to estimate ultimate losses
-- and IBNR (Incurred But Not Reported) reserves.

SELECT
    accident_year,
    development_period AS dev_period_yrs,
    line_of_business,
    incremental_paid_eur_m,
    cumulative_paid_eur_m,
    case_reserves_eur_m,
    ibnr_eur_m,
    ultimate_loss_eur_m,
    ROUND(cumulative_paid_eur_m / NULLIF(ultimate_loss_eur_m, 0) * 100, 1)
        AS pct_paid_to_ultimate
FROM serverless_stable_xhky6g_catalog.allianz_silver.pc_loss_development
ORDER BY accident_year DESC, dev_period_yrs;
