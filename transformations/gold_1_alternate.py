"""
Q1 (ALTERNATE, PySpark) -- same logic as gold_population_stats_2013_2018
(03_gold_q1_population_stats.sql), reimplemented in the DataFrame API.

This table is NOT consumed downstream. It exists purely so the SQL and
PySpark versions can be diffed against each other:

    SELECT * FROM gold_population_stats_2013_2018
    EXCEPT
    SELECT mean_population, stddev_population, num_years
    FROM gold_population_stats_2013_2018_alt_pyspark
    -- should return zero rows
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="gold_population_stats_2013_2018_alt_pyspark",
    comment="ALTERNATE implementation of Q1 in PySpark. Not consumed downstream -- for parity review against gold_population_stats_2013_2018.",
)
def gold_population_stats_2013_2018_alt_pyspark():
    pop = spark.read.table("silver_population").filter(
        (F.col("nation") == "United States") &
        (F.col("year") >= 2013) & (F.col("year") <= 2018)
    )
    return pop.agg(
        F.avg("population").alias("mean_population"),
        F.stddev_samp("population").alias("stddev_population"),
        F.count("*").alias("num_years"),
    )