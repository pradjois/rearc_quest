"""
Q2 (PRIMARY, PySpark) -- for every series_id, the "best year": the year
with the largest sum of `value` across that series' quarters.

Only period IN ('Q01','Q02','Q03','Q04') is summed -- the Q05 annual-average
row is excluded so it isn't double-counted against the four quarters.
Ties are broken by preferring the earlier year (deterministic; see README).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

QUARTER_PERIODS = ("Q01", "Q02", "Q03", "Q04")


@dp.table(
    name="gold_series_best_year_alt_pyspark",
    comment="For every series_id, the year with the largest sum of value across its quarters (Q01-Q04), with a human-readable label. PRIMARY implementation (PySpark) -- feeds downstream consumers.",
)
def gold_series_best_year():
    quarterly = spark.read.table("silver_bls_pr_data").filter(
        F.col("period").isin(*QUARTER_PERIODS)
    )

    yearly_sums = quarterly.groupBy("series_id", "year").agg(
        F.sum("value").alias("summed_value")
    )

    # Rank years within each series_id by summed_value desc; earliest year
    # wins ties.
    w = Window.partitionBy("series_id").orderBy(
        F.col("summed_value").desc(), F.col("year").asc()
    )
    best_year = (
        yearly_sums.withColumn("rk", F.row_number().over(w))
        .filter(F.col("rk") == 1)
        .drop("rk")
    )

    series = spark.read.table("silver_bls_pr_series").select("series_id", "series_label")

    return best_year.join(series, on="series_id", how="left").select(
        "series_id",
        F.coalesce(F.col("series_label"), F.lit("Unknown series")).alias("series_label"),
        F.col("year").alias("best_year"),
        "summed_value",
    )