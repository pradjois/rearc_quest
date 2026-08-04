-- Q3 (PRIMARY, Spark SQL): for series_id = PRS30006032 and period = Q01,
-- the value each year, left-joined with that year's population.
--
-- LEFT JOIN so years with no population match still appear (population =
-- NULL), matching "joined with that year's population where available".
CREATE OR REFRESH MATERIALIZED VIEW gold_prs30006032_q1_vs_population
COMMENT 'PRS30006032, period Q01: value per year left-joined with that year''s population. PRIMARY implementation (Spark SQL) -- feeds downstream consumers.'
AS
SELECT
  d.year,
  d.value AS q1_value,
  p.population
FROM silver_bls_pr_data d
LEFT JOIN silver_population p
  ON d.year = p.year
WHERE d.series_id = 'PRS30006032'
  AND d.period = 'Q01'
ORDER BY d.year;