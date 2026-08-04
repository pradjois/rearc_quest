# BLS Productivity — Spark Declarative Pipeline

Bronze → Silver → Gold pipeline over the BLS Productivity (`pr`) time-series
files and the Data USA national population API, built with **Lakeflow /
Spark Declarative Pipelines** (`pyspark.pipelines`, formerly `dlt`).

## Before you run it

1. Replace every `<catalog>.<schema>.<volume>` / `/Volumes/<catalog>/<schema>/<volume>`
   placeholder with your real Unity Catalog path (search for `<catalog>` across
   the `transformations/` folder).
2. Confirm the raw files already landed under that Volume, using the layout
   from the download step:
   ```
   /Volumes/<catalog>/<schema>/<volume>/bls/pr/pr.class
   /Volumes/<catalog>/<schema>/<volume>/bls/pr/pr.data.1.AllData
   /Volumes/<catalog>/<schema>/<volume>/bls/pr/pr.duration
   /Volumes/<catalog>/<schema>/<volume>/bls/pr/pr.measure
   /Volumes/<catalog>/<schema>/<volume>/bls/pr/pr.seasonal
   /Volumes/<catalog>/<schema>/<volume>/bls/pr/pr.sector
   /Volumes/<catalog>/<schema>/<volume>/bls/pr/pr.series
   /Volumes/<catalog>/<schema>/<volume>/population/population_acs_yg_total_population_1.json
   ```
3. Create a pipeline pointing at this `transformations/` folder (Workflows →
   Pipelines → Create → source code = this folder), set a target
   catalog/schema, and run an update (Development mode while iterating).

## Layer map
Generally
  - bronze : ingest raw data
  - silver : cleaned data
  - gold : metrics

| Layer  | Table(s) | Purpose |
|---|---|---|
| Bronze | `bronze_bls_pr_data`, `bronze_bls_pr_series`, `bronze_bls_pr_sector`, `bronze_bls_pr_class`, `bronze_bls_pr_measure`, `bronze_bls_pr_duration`, `bronze_bls_pr_seasonal`, `bronze_population` | Raw files landed with minimal typing (mostly strings), one dataset per source file. |
Ideally we cast/clean the data with checks, materialized view may not be needed.
| Silver | `silver_bls_pr_data`, `silver_bls_pr_series`, `silver_population` | Typed, `EXPECT`-validated, deduplicated logic. `silver_bls_pr_series` adds `series_label`. |
| Gold   | `gold_population_stats_2013_2018`, `gold_series_best_year`, `gold_prs30006032_q1_vs_population` | The three analytical answers from sql as primary due to flexibility|

## The three questions → Gold tables

Each question is implemented **twice** — once in Spark SQL, once in
PySpark. One is wired in as the table the pipeline actually publishes
(the *primary*); the other is registered as a `*_alt_*` table so it's
runnable and diffable against the primary, but nothing downstream reads it.

| Q | Primary Gold table | Language | Alternate table | Language |
|---|---|---|---|---|
| 1. Mean/stddev of US population, 2013–2018 | `gold_population_stats_2013_2018` | SQL (`03_gold_q1_population_stats.sql`) | `gold_population_stats_2013_2018_alt_pyspark` | PySpark (`03_gold_q1_population_stats_alt.py`) |
| 2. Best year per `series_id` (sum of quarterly `value`) | `gold_series_best_year` | PySpark (`04_gold_q2_series_best_year.py`) | `gold_series_best_year_alt_sql` | SQL (`04_gold_q2_series_best_year_alt.sql`) |
| 3. `PRS30006032`, period `Q01`, value by year + population | `gold_prs30006032_q1_vs_population` | SQL (`05_gold_q3_prs30006032_q1_population.sql`) | `gold_prs30006032_q1_vs_population_alt_pyspark` | PySpark (`05_gold_q3_prs30006032_q1_population_alt.py`) |

Sanity check after running: `SELECT * FROM gold_population_stats_2013_2018
EXCEPT SELECT * FROM gold_population_stats_2013_2018_alt_pyspark` (and the
equivalent for the other two pairs) should return zero rows.

## Notes on Q2 ("best year")

- "Across all its quarters" is read as `period IN ('Q01','Q02','Q03','Q04')`
  — the annual-average row (`period = 'Q05'`) is excluded from both the sum
  and the candidate years, since including it would double-count.
- Ties (two years with the same summed value) are broken by picking the
  earlier year, for determinism. Change the `ORDER BY` in the window/rank
  logic if you'd rather keep ties or pick the later year.
- `series_label` is a left join, so a `series_id` with a code BLS hasn't
  published a description for still comes through (with a `NULL` piece in
  the label) rather than being dropped.

## Notes on Q3

- Implemented as a `LEFT JOIN` from the BLS side to population, per the
  question ("joined with that year's population **where available**") —
  years without a population match keep the row with `population = NULL`
  rather than being filtered out.