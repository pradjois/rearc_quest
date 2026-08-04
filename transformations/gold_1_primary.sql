-- Q1 (PRIMARY, Spark SQL): mean and standard deviation of the annual US
-- population across 2013-2018 inclusive.
--
-- STDDEV_SAMP (sample stddev, N-1 denominator) is used since these six
-- years are a sample of the population time series, not the full
-- population of possible years -- swap to STDDEV_POP if a population
-- (N-denominator) statistic is what's wanted instead.
CREATE OR REFRESH MATERIALIZED VIEW gold_population_stats_2013_2018
COMMENT 'Mean and standard deviation of US population, 2013-2018 inclusive. PRIMARY implementation (Spark SQL) -- feeds downstream consumers.'
AS
SELECT
  AVG(population)         AS mean_population,
  STDDEV_SAMP(population)  AS stddev_population,
  COUNT(*)                AS num_years
FROM silver_population
WHERE nation = 'United States'
  AND year BETWEEN 2013 AND 2018;