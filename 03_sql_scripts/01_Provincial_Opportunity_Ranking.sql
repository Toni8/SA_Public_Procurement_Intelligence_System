USE procurement_intelligence;

-- Question 1: Provincial Opportunity Ranking

WITH provincial_summary AS (
    SELECT 
        m.tender_province AS province,
        COUNT(DISTINCT m.ocid) AS tender_count,
        COUNT(c.id)           AS contracts_awarded,
        SUM(c.value_amount)   AS total_value,
        AVG(c.value_amount)   AS avg_value
    FROM main_staging m
    INNER JOIN contracts_staging c 
        ON m.ocid = c.main_ocid
    WHERE c.value_amount > 0 
      AND m.tender_province IS NOT NULL
    GROUP BY m.tender_province
)
SELECT 
    province,
    tender_count,
    contracts_awarded,
    ROUND(total_value / 1000000000, 3)                  AS total_value_billions,
    ROUND(avg_value / 1000000, 2)                       AS avg_contract_millions,
    ROUND(total_value * 100.0 / SUM(total_value) OVER (), 2) AS pct_of_national_spend,

    RANK() OVER (ORDER BY total_value DESC)             AS value_rank,
    RANK() OVER (ORDER BY tender_count DESC)            AS volume_rank,
    
    -- Safe subtraction using CAST AS SIGNED
    CAST(RANK() OVER (ORDER BY tender_count DESC) AS SIGNED) 
    - 
    CAST(RANK() OVER (ORDER BY total_value DESC) AS SIGNED) AS rank_gap

FROM provincial_summary
ORDER BY total_value DESC;
