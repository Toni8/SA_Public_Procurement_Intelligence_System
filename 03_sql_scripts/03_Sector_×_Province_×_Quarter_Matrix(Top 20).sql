-- Question 3: Sector × Province × Quarter Matrix (Top 20)
SELECT 
    m.tender_mainprocurementcategory AS sector,
    m.tender_province AS province,
    QUARTER(m.date) AS quarter,
    COUNT(c.id) AS contract_count,
    ROUND(AVG(c.value_amount) / 1e6, 2) AS avg_value_millions,
    ROUND(SUM(c.value_amount) / 1e6, 2) AS total_value_millions,
    ROUND(MAX(c.value_amount) / 1e6, 2) AS max_contract_millions,
    CASE 
        WHEN COUNT(c.id) >= 5 THEN 'Reliable'
        WHEN COUNT(c.id) >= 2 THEN 'Caution'
        ELSE 'Insufficient Data'
    END AS reliability_tier,
    CASE 
        WHEN COUNT(c.id) >= 5 THEN ROUND(AVG(c.value_amount) / 1e6, 2)
        WHEN COUNT(c.id) >= 2 THEN ROUND(AVG(c.value_amount) / 1e6 * 0.5, 2)
        ELSE 0
    END AS weighted_opportunity_score
FROM main_staging m
INNER JOIN contracts_staging c ON m.ocid = c.main_ocid
WHERE c.value_amount > 0 
  AND m.tender_province IS NOT NULL 
  AND m.tender_mainprocurementcategory IS NOT NULL
  AND m.date IS NOT NULL
GROUP BY m.tender_mainprocurementcategory, m.tender_province, QUARTER(m.date)
HAVING reliability_tier != 'Insufficient Data'
ORDER BY weighted_opportunity_score DESC
LIMIT 20;
