-- Question 6: Realistic Value Benchmarks by Sector + Province
SELECT 
    m.tender_mainprocurementcategory AS sector,
    m.tender_province AS province,
    COUNT(c.id) AS sample_size,
    ROUND(AVG(c.value_amount) / 1000000, 2) AS mean_millions,
    ROUND(MIN(c.value_amount) / 1000000, 2) AS min_millions,
    ROUND(MAX(c.value_amount) / 1000000, 2) AS max_millions,
    ROUND(STDDEV(c.value_amount) / 1000000, 2) AS std_dev_millions,
    CASE 
        WHEN COUNT(c.id) >= 10 THEN 'High reliability'
        WHEN COUNT(c.id) >= 5  THEN 'Medium reliability'
        ELSE 'Low reliability'
    END AS benchmark_reliability
FROM main_staging m
INNER JOIN contracts_staging c ON m.ocid = c.main_ocid
WHERE c.value_amount > 0 
  AND m.tender_mainprocurementcategory IS NOT NULL
  AND m.tender_province IS NOT NULL
GROUP BY m.tender_mainprocurementcategory, m.tender_province
HAVING COUNT(c.id) >= 5
ORDER BY mean_millions DESC;
