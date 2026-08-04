"""
Bronze layer -- Data USA / Census ACS national population API.

The API returns {"data": [ {...one record per Year...}, ... ]}. Bronze just
explodes that array into rows and keeps every field the API returned, still
as the types the JSON parser inferred (ints stay ints here since JSON is
already typed, unlike the tab-delimited BLS files).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# EDIT ME: point at your actual Unity Catalog volume.
VOLUME_BASE = "/Volumes/dmp/rearc/quest/bls/pr"
POPULATION_FILE = f"{VOLUME_BASE}/population.json"


@dp.table(
    name="bronze_population",
    comment="Raw Data USA ACS national population-by-year records, exploded from the API's JSON response.",
)
def bronze_population():
    raw = spark.read.option("multiLine", "true").json(POPULATION_FILE)
    return (
        raw.select(F.explode("data").alias("row"))
        .select("row.*")
        # Rename columns with spaces to underscores
        .withColumnRenamed("Nation ID", "Nation_ID")
        .withColumn("_source_file", F.lit("population.json"))
        .withColumn("_ingest_ts", F.current_timestamp())
    )