-- Silver layer: typed, expectation-checked datasets built on top of Bronze.

-- ---------------------------------------------------------------------------
-- Quarterly + annual PR values, typed and quality-checked.
-- Q05 (annual average) rows are kept here -- filtering to quarters only
-- happens downstream in Gold, since Silver should stay a faithful, typed
-- copy of everything Bronze handed it.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW silver_bls_pr_data (
  CONSTRAINT valid_series_id EXPECT (series_id IS NOT NULL AND series_id != '') ON VIOLATION DROP ROW,
  CONSTRAINT valid_year      EXPECT (year IS NOT NULL)                          ON VIOLATION DROP ROW,
  CONSTRAINT valid_period    EXPECT (period IS NOT NULL AND period != '')       ON VIOLATION DROP ROW,
  CONSTRAINT valid_value     EXPECT (value IS NOT NULL)
)
COMMENT 'Typed BLS PR values: one row per series_id/year/period.'
AS
SELECT
  series_id,
  CAST(year AS INT)     AS year,
  period,
  CAST(value AS DOUBLE) AS value,
  NULLIF(footnote_codes, '') AS footnote_codes
FROM dmp.rearc.bronze_bls_pr_data;

-- ---------------------------------------------------------------------------
-- Series registry enriched with a human-readable label. pr.series itself
-- only has codes (see README) -- series_label is built dynamically from
-- whatever descriptions BLS's own lookup files currently contain.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW silver_bls_pr_series (
  CONSTRAINT valid_series_id EXPECT (series_id IS NOT NULL AND series_id != '') ON VIOLATION DROP ROW
)
COMMENT 'One row per BLS PR series_id with classification codes and a human-readable series_label.'
AS
SELECT
  s.series_id,
  s.sector_code,
  sec.sector_name,
  s.class_code,
  cls.class_name,
  s.measure_code,
  mea.measure_name,
  s.duration_code,
  dur.duration_name,
  s.seasonal,
  seas.seasonal_name,
  s.begin_year,
  s.begin_period,
  s.end_year,
  s.end_period,
  CONCAT_WS(
    ' — ',
    sec.sector_name,
    cls.class_name,
    mea.measure_name,
    dur.duration_name,
    seas.seasonal_name
  ) AS series_label
FROM dmp.rearc.bronze_bls_pr_series s
LEFT JOIN bronze_bls_pr_sector   sec  ON s.sector_code   = sec.sector_code
LEFT JOIN bronze_bls_pr_class    cls  ON s.class_code    = cls.class_code
LEFT JOIN bronze_bls_pr_measure  mea  ON s.measure_code  = mea.measure_code
LEFT JOIN bronze_bls_pr_duration dur  ON s.duration_code = dur.duration_code
LEFT JOIN bronze_bls_pr_seasonal seas ON s.seasonal      = seas.seasonal_code;

-- ---------------------------------------------------------------------------
-- National population by year.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW silver_population (
  CONSTRAINT valid_year       EXPECT (year IS NOT NULL)                      ON VIOLATION DROP ROW,
  CONSTRAINT valid_population EXPECT (population IS NOT NULL AND population > 0) ON VIOLATION DROP ROW
)
COMMENT 'US national population by year, from the Data USA ACS API.'
AS
SELECT
  CAST(Year AS INT)          AS year,
  Nation                     AS nation,
  CAST(Population AS BIGINT) AS population
FROM dmp.rearc.bronze_population;