-- Question 4: Dominant Suppliers & Concentration 
WITH supplier_totals AS (
    SELECT 
        s.name AS supplier,
        COUNT(DISTINCT a.id)                    AS total_awards,
        ROUND(SUM(a.value_amount) / 1000000, 2) AS total_value_millions,
        ROUND(AVG(a.value_amount) / 1000000, 2) AS avg_award_millions,
        ROUND(MAX(a.value_amount) / 1000000, 2) AS largest_single_award
    FROM awards_suppliers_staging s
    INNER JOIN awards_staging a 
        ON s.awards_id = a.id
    WHERE a.value_amount IS NOT NULL 
      AND a.value_amount > 0
    GROUP BY s.name
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY total_value_millions DESC) AS spend_rank,
        SUM(total_value_millions) OVER () AS grand_total
    FROM supplier_totals
)
SELECT 
    spend_rank,
    supplier,
    total_awards,
    total_value_millions,
    avg_award_millions,
    largest_single_award,
    ROUND(total_value_millions * 100.0 / grand_total, 2) AS pct_of_total_spend,
    ROUND(SUM(total_value_millions) 
          OVER (ORDER BY spend_rank) * 100.0 / grand_total, 2) AS cumulative_pct
FROM ranked
WHERE spend_rank <= 20
ORDER BY spend_rank;
