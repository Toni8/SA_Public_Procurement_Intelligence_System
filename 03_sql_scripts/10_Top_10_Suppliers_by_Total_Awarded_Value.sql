-- Question 10 : Top 10 Suppliers by Total Awarded Value 
WITH supplier_summary AS (
    SELECT 
        s.name AS supplier_name,
        COUNT(DISTINCT a.id) AS total_awards,
        ROUND(SUM(a.value_amount) / 1000000, 2) AS total_value_millions,
        ROUND(AVG(a.value_amount) / 1000000, 2) AS avg_award_millions,
        ROUND(MAX(a.value_amount) / 1000000, 2) AS largest_award_millions,
        COUNT(DISTINCT m.tender_province) AS provinces_active,
        COUNT(DISTINCT m.tender_mainprocurementcategory) AS sectors_active
    FROM awards_suppliers_staging s
    INNER JOIN awards_staging a ON s.awards_id = a.id
    INNER JOIN main_staging m ON a.main_ocid = m.ocid
    WHERE a.value_amount > 0
    GROUP BY s.name
)
SELECT 
    RANK() OVER (ORDER BY total_value_millions DESC) AS `rank`,
    supplier_name,
    total_awards,
    total_value_millions,
    avg_award_millions,
    largest_award_millions,
    provinces_active,
    sectors_active
FROM supplier_summary
ORDER BY total_value_millions DESC
LIMIT 10;
