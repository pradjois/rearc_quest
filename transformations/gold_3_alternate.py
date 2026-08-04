"""
Q3 (ALTERNATE, PySpark) -- same logic as gold_prs30006032_q1_vs_population
(05_gold_q3_prs30006032_q1_population.sql), reimplemented in the DataFrame
API.

NOT consumed downstream. Diff against the primary with:
    SELECT * FROM gold_prs30006032_q1_vs_population
    EXCEPT
    SELECT * FROM gold_prs30006032_q1_vs_population_alt_pyspark
    -- should return zero rows
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET_SERIES_ID = "PRS30006032"
TARGET_PERIOD = "Q01"


@dp.table(
    name="gold_prs30006032_q1_vs_population_alt_pyspark",
    comment="ALTERNATE implementation of Q3 in PySpark. Not consumed downstream -- for parity review against gold_prs30006032_q1_vs_population.",
)
def gold_prs30006032_q1_vs_population_alt_pyspark():
    series_values = spark.read.table("silver_bls_pr_data").filter(
        (F.col("series_id") == TARGET_SERIES_ID) & (F.col("period") == TARGET_PERIOD)
    )
    population = spark.read.table("silver_population")

    return (
        series_values.join(population, on="year", how="left")
        .select(
            "year",
            F.col("value").alias("q1_value"),
            "population",
        )
        .orderBy("year")
    )