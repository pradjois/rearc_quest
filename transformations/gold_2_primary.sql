-- Q2 (ALTERNATE, Spark SQL) -- same logic as gold_series_best_year
-- (04_gold_q2_series_best_year.py), reimplemented with a window function.
--
-- NOT consumed downstream. Diff against the primary with:
--   SELECT * FROM gold_series_best_year
--   EXCEPT
--   SELECT * FROM gold_series_best_year_alt_sql
--   -- should return zero rows

CREATE OR REFRESH MATERIALIZED VIEW gold_series_best_year
COMMENT 'implementation of Q2 in Spark SQL. Not consumed downstream -- for parity review against gold_series_best_year.'
AS
WITH quarterly_sums AS (
  SELECT
    series_id,
    year,
    SUM(value) AS summed_value
  FROM silver_bls_pr_data
  WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
  GROUP BY series_id, year
),
ranked AS (
  SELECT
    series_id,
    year,
    summed_value,
    ROW_NUMBER() OVER (
      PARTITION BY series_id
      ORDER BY summed_value DESC, year ASC
    ) AS rk
  FROM quarterly_sums
)
SELECT
  r.series_id,
  COALESCE(s.series_label, 'Unknown series') AS series_label,
  r.year AS best_year,
  r.summed_value
FROM ranked r
LEFT JOIN silver_bls_pr_series s ON r.series_id = s.series_id
WHERE r.rk = 1;