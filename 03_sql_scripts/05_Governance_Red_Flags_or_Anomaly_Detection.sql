-- Question 5: Governance Red Flags / Anomaly Detection
SELECT 
    m.ocid,
    m.buyer_name,
    m.tender_province AS province,
    m.tender_mainprocurementcategory AS sector,
    m.tender_procurementmethod AS method,
    s.name AS supplier,
    ROUND(c.value_amount / 1e6, 2) AS contract_value_millions,
    m.date AS tender_date,
    CASE 
        WHEN m.tender_procurementmethod IN ('direct', 'limited', 'selective') 
             AND c.value_amount > 15000000 
             THEN 'FLAG 1 — Restricted method above R15M'
        WHEN c.value_amount > 100000000 
             AND m.tender_procurementmethod != 'open' 
             THEN 'FLAG 2 — Non-open mega contract'
        WHEN c.value_amount > 50000000 
             AND s.name IN (
                 SELECT name 
                 FROM awards_suppliers_staging 
                 GROUP BY name 
                 HAVING COUNT(DISTINCT awards_id) = 1
             )
             THEN 'FLAG 3 — Single large award, no prior history'
        WHEN c.value_amount > 200000000 
             THEN 'FLAG 4 — Mega contract above R200M'
        ELSE 'Normal'
    END AS anomaly_flag
FROM main_staging m
INNER JOIN contracts_staging c ON m.ocid = c.main_ocid
LEFT JOIN awards_staging a ON m.ocid = a.main_ocid
LEFT JOIN awards_suppliers_staging s ON a.id = s.awards_id
WHERE c.value_amount > 0
HAVING anomaly_flag != 'Normal'
ORDER BY contract_value_millions DESC;
