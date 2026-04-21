-- Question 9: Market Trends Over Time
SELECT 
    YEAR(m.date) AS year,
    COUNT(DISTINCT m.ocid) AS total_tenders,
    COUNT(c.id) AS contracts_awarded,
    ROUND(SUM(c.value_amount) / 1e9, 3) AS total_value_billions,
    ROUND(SUM(CASE WHEN m.tender_procurementmethod = 'open' THEN 1 ELSE 0 END) * 100.0 / 
          COUNT(DISTINCT m.ocid), 1) AS open_tender_pct,
    ROUND(SUM(CASE WHEN m.tender_procurementmethod IN ('direct','limited','selective') 
                   THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT m.ocid), 1) AS restricted_tender_pct
FROM main_staging m
LEFT JOIN contracts_staging c ON m.ocid = c.main_ocid AND c.value_amount > 0
WHERE m.date IS NOT NULL 
  AND YEAR(m.date) >= 2021
GROUP BY YEAR(m.date)
ORDER BY year;
