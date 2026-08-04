"""
Bronze layer -- BLS Productivity (pr) flat files.

Every table here is a near-verbatim landing of one source file: values are
trimmed (BLS pads columns with tabs/spaces) but not cast, filtered, or
deduplicated. That happens in Silver. This keeps Bronze replayable straight
from the Volume with no business logic baked in.
"""


from pyspark import pipelines as dp
from pyspark.sql import functions as F, DataFrame

# EDIT ME: point at your actual Unity Catalog volume.
VOLUME_BASE = "/Volumes/dmp/rearc/quest/bls"

def _read_tab_delimited(path: str, source_file: str) -> DataFrame:
    """Read a tab-delimited BLS file, trim whitespace from every column and
    every header name, and stamp on basic lineage columns."""
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", "\t")
        .option("inferSchema", "false")
        .load(path)
    )
    df = df.toDF(*[c.strip() for c in df.columns])
    df = df.select([F.trim(F.col(c)).alias(c) for c in df.columns])
    return (
        df.withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingest_ts", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# The values table itself: series_id / year / period / value / footnote_codes
# ---------------------------------------------------------------------------
@dp.table(
    name="bronze_bls_pr_data",
    comment="Raw BLS PR values -- one row per series_id/year/period (quarters + annual average).",
)
def bronze_bls_pr_data():
    return _read_tab_delimited(f"{VOLUME_BASE}/pr/pr.data.1.AllData", "pr.data.1.AllData")


# ---------------------------------------------------------------------------
# Series metadata: NOTE this file has NO series_title / description column.
# It only carries the numeric/letter codes -- the lookup tables below are
# what turn those codes into something a human can read.
# Columns: series_id, sector_code, class_code, measure_code, duration_code,
#          seasonal, base_year, footnote_codes, begin_year, begin_period,
#          end_year, end_period
# ---------------------------------------------------------------------------
@dp.table(
    name="bronze_bls_pr_series",
    comment="Raw BLS PR series registry -- one row per series_id, coded (not yet human-readable).",
)
def bronze_bls_pr_series():
    return _read_tab_delimited(f"{VOLUME_BASE}/pr/pr.series", "pr.series")


# ---------------------------------------------------------------------------
# Code -> description lookup files. Each is a small two-column file.
# Renamed positionally (first column = code, second = description) since
# BLS doesn't keep header spelling consistent across these lookup files.
# ---------------------------------------------------------------------------
def _read_lookup(path: str, source_file: str, code_col: str, name_col: str) -> DataFrame:
    df = _read_tab_delimited(path, source_file)
    cols = [c for c in df.columns if not c.startswith("_source_file") and not c.startswith("_ingest_ts")]
    # first real column -> code, second real column -> description
    renamed = (
        df.withColumnRenamed(cols[0], code_col)
        .withColumnRenamed(cols[1], name_col)
    )
    return renamed.select(code_col, name_col, "_source_file", "_ingest_ts")


@dp.table(name="bronze_bls_pr_sector", comment="Raw sector_code -> sector description lookup.")
def bronze_bls_pr_sector():
    return _read_lookup(f"{VOLUME_BASE}/pr/pr.sector", "pr.sector", "sector_code", "sector_name")


@dp.table(name="bronze_bls_pr_class", comment="Raw class_code -> class description lookup.")
def bronze_bls_pr_class():
    return _read_lookup(f"{VOLUME_BASE}/pr/pr.class", "pr.class", "class_code", "class_name")


@dp.table(name="bronze_bls_pr_measure", comment="Raw measure_code -> measure description lookup.")
def bronze_bls_pr_measure():
    return _read_lookup(f"{VOLUME_BASE}/pr/pr.measure", "pr.measure", "measure_code", "measure_name")


@dp.table(name="bronze_bls_pr_duration", comment="Raw duration_code -> duration description lookup.")
def bronze_bls_pr_duration():
    return _read_lookup(f"{VOLUME_BASE}/pr/pr.duration", "pr.duration", "duration_code", "duration_name")


@dp.table(name="bronze_bls_pr_seasonal", comment="Raw seasonal code -> seasonal-adjustment description lookup.")
def bronze_bls_pr_seasonal():
    return _read_lookup(f"{VOLUME_BASE}/pr/pr.seasonal", "pr.seasonal", "seasonal_code", "seasonal_name")