from pyspark.sql import DataFrame
from pyspark.sql.functions import count, max as spark_max, col

def aggregate_gold(df_silver: DataFrame) -> DataFrame:
    return (
        df_silver.groupBy("country", "state", "brewery_type")
            .agg(
                count("*").alias("brewery_count"),
                spark_max("ingest_date").alias("last_ingest_date")
            )
            .orderBy(col("brewery_count").desc())
    )
