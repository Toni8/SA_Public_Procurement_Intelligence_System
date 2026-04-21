-- Question 8: Top Buyers Intelligence
SELECT 
    m.buyer_name,
    m.tender_province AS province,
    m.tender_mainprocurementcategory AS sector,
    COUNT(DISTINCT m.ocid) AS tenders_issued,
    COUNT(c.id) AS contracts_awarded,
    ROUND(SUM(c.value_amount) / 1e6, 2) AS total_spend_millions,
    ROUND(AVG(c.value_amount) / 1e6, 2) AS avg_contract_millions,
    ROUND(COUNT(c.id) * 100.0 / NULLIF(COUNT(DISTINCT m.ocid), 0), 1) AS award_conversion_rate_pct,
    CASE 
        WHEN AVG(c.value_amount) > 50000000 THEN 'Tier 1 — Strategic'
        WHEN AVG(c.value_amount) > 10000000 THEN 'Tier 2 — High Value'
        WHEN AVG(c.value_amount) > 1000000  THEN 'Tier 3 — Mid Market'
        ELSE 'Tier 4 — Small'
    END AS buyer_tier
FROM main_staging m
INNER JOIN contracts_staging c ON m.ocid = c.main_ocid
WHERE c.value_amount > 0
GROUP BY m.buyer_name, m.tender_province, m.tender_mainprocurementcategory
HAVING COUNT(c.id) >= 2
ORDER BY total_spend_millions DESC
LIMIT 25;
