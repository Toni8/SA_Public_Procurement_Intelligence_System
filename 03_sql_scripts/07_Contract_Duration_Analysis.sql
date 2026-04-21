-- Question 7: Contract Duration Analysis
SELECT 
    m.tender_mainprocurementcategory AS sector,
    CASE 
        WHEN c.period_durationindays <= 90   THEN 'Under 3 months'
        WHEN c.period_durationindays <= 180  THEN '3-6 months'
        WHEN c.period_durationindays <= 365  THEN '6-12 months'
        WHEN c.period_durationindays <= 730  THEN '1-2 years'
        WHEN c.period_durationindays <= 1095 THEN '2-3 years'
        ELSE 'Over 3 years'
    END AS duration_band,
    COUNT(c.id) AS contract_count,
    ROUND(AVG(c.value_amount) / 1e6, 2) AS avg_value_millions,
    ROUND(AVG(c.period_durationindays), 0) AS avg_days,
    ROUND(COUNT(c.id) * 100.0 / SUM(COUNT(c.id)) OVER (PARTITION BY m.tender_mainprocurementcategory), 2) AS pct_within_sector
FROM main_staging m
INNER JOIN contracts_staging c ON m.ocid = c.main_ocid
WHERE c.period_durationindays > 0 
  AND c.value_amount > 0
GROUP BY m.tender_mainprocurementcategory, duration_band
ORDER BY sector, duration_band;
