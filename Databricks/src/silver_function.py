from pyspark.sql import DataFrame
from pyspark.sql.functions import trim, col, lit, current_timestamp
from pyspark.sql.types import DoubleType
from datetime import datetime, UTC

def transform_silver(df_bronze: DataFrame, ingest_date: str) -> DataFrame:
    return (
        df_bronze
            .select(
                trim(col("address_1")).alias("address_1"),
                trim(col("address_2")).alias("address_2"),
                trim(col("address_3")).alias("address_3"),
                trim(col("brewery_type")).alias("brewery_type"),
                trim(col("city")).alias("city"),
                trim(col("country")).alias("country"),
                trim(col("id")).alias("id"),
                col("latitude").cast(DoubleType()).alias("latitude"),
                col("longitude").cast(DoubleType()).alias("longitude"),
                trim(col("name")).alias("name"),
                trim(col("phone")).alias("phone"),
                trim(col("postal_code")).alias("postal_code"),
                trim(col("state")).alias("state"),
                trim(col("state_province")).alias("state_province"),
                trim(col("street")).alias("street"),
                trim(col("website_url")).alias("website_url"),
                lit(ingest_date).alias("ingest_date"),
                current_timestamp().alias("ingest_timestamp")
            )
    )
