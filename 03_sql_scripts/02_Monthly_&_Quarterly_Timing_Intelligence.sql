-- Question 2: Monthly & Quarterly Timing Intelligence
SELECT 
    QUARTER(m.date) AS quarter,
    MONTHNAME(m.date) AS month_name,
    MONTH(m.date) AS month_num,
    COUNT(DISTINCT m.ocid) AS tenders_issued,
    COUNT(c.id) AS contracts_awarded,
    ROUND(SUM(c.value_amount) / 1e6, 2) AS total_value_millions,
    ROUND(AVG(c.value_amount) / 1e6, 2) AS avg_contract_millions,
    ROUND(COUNT(DISTINCT m.ocid) * 100.0 / 
          SUM(COUNT(DISTINCT m.ocid)) OVER (), 2) AS pct_of_annual_volume,
    CASE 
        WHEN COUNT(DISTINCT m.ocid) >= AVG(COUNT(DISTINCT m.ocid)) OVER () * 1.2 THEN 'Peak — Full bid capacity'
        WHEN COUNT(DISTINCT m.ocid) <= AVG(COUNT(DISTINCT m.ocid)) OVER () * 0.6 THEN 'Dead zone — Pipeline prep'
        ELSE 'Active — Normal operations'
    END AS bid_team_recommendation
FROM main_staging m
LEFT JOIN contracts_staging c ON m.ocid = c.main_ocid
WHERE m.date IS NOT NULL
GROUP BY QUARTER(m.date), MONTHNAME(m.date), MONTH(m.date)
ORDER BY month_num;
