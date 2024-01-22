# 
# Note: This job implementation is prone to errors when upstream changes.
# (Different JSON files with more or less rows -> Wrong dim_id for old data)
# Therefore, this is just a temporary implementation with fixed JSON files.
# 

from typing import List

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.errors import AnalysisException

from configs import HDFSConfig, WebHDFSConfig, SparkConfig

import hdfs
import json


def main() -> None:

    """Load aircrafts dimension table"""
    # Get configs
    SPARK_CONF = SparkConfig()
    HDFS_CONF = HDFSConfig()
    WEBHDFS_CONF = WebHDFSConfig()
    hdfs_path = "/data_lake"
    dir_uri = HDFS_CONF.uri + hdfs_path
    
    # Create SparkSession
    spark = SparkSession.builder \
        .master(SPARK_CONF.uri) \
        .config("spark.sql.warehouse.dir", SPARK_CONF.sql_warehouse_dir) \
        .enableHiveSupport() \
        .getOrCreate()

    # Get aircrafts data from data lake and preprocess
    df_aircrafts = preprocess_aircrafts(
        spark.read.csv(
            dir_uri + "/aircraft-database-complete-2024-01.csv", 
            schema = SPARK_CONF.schema.src_aircrafts
        )
    )
    
    # Preprocess manufacturer data
    df_manufacturers = preprocess_manufacturers(
        spark.read.csv(
            dir_uri + "/doc8643Manufacturers.csv",
            schema = SPARK_CONF.schema.src_manufacturers
        )
    )
    
    # Preprocess aircraft type data
    df_aircraft_types = preprocess_aircraft_types(
        spark.read.csv(
            dir_uri + "/doc8643AircraftTypes.csv",
            schema = SPARK_CONF.schema.src_aircraft_types
        )
    )
    
    # Read and preprocess airline data
    hdfs_path = "/data_lake/airlines.json"
    client = hdfs.InsecureClient(WEBHDFS_CONF.uri)
    with client.read(hdfs_path) as file:
        airlines = json.load(file)["rows"]
    
    df_airlines = preprocess_airlines(
        spark.createDataFrame(
            airlines, 
            schema = SPARK_CONF.schema.src_airlines
        )
    )

    # Augment aircraft data - 
    # Drop columns outside of final DataFrame in the process 
    df_aircrafts = df_aircrafts \
        .join(
            df_manufacturers,
            on = (df_aircrafts["manufacturer_code"] \
                == df_manufacturers["code"]),
            how = "left"
        ) \
        .drop("manufacturer_code", "code") \
        .join(
            df_aircraft_types,
            on = (df_aircrafts["icao_type"] \
                == df_aircraft_types["icao_type_code"]),
            how = "left"
        ) \
        .drop("icao_type_code") \
        .join(
            df_airlines,
            on = ~df_airlines["identifier"].isNull() &
                (df_aircrafts["operator_identifier"] \
                == df_airlines["identifier"]),
            how = "left"
        ) \
        .withColumn(
            "operating_airline",
            F.when(F.col("operating_airline").isNull(), F.col("operator_name"))
        ) \
        .drop(
            "operator_name", 
            "operator_icao", 
            "operator_iata", 
            "operator_identifier",
            "identifier"
        )
    
    # Add dim_id column and rearrange columns order
    df_aircrafts = df_aircrafts \
        .withColumn(
            "aircraft_dim_id", 
            F.row_number().over(Window.orderBy("icao24_addr"))
        )

    # Quality check - if filter too much, cannot represent dimension data in fact
    df_aircrafts.filter(~F.col("operating_airline").isNull()).show()

    df_flights = spark.read.parquet(HDFS_CONF.uri + "/data_lake/flights/year=2018/month=1/day=1")
    df = df_flights.join(
        df_aircrafts, 
        on = (df_flights["icao24"] == df_aircrafts["icao24_addr"]),
        how = "left"
    )

    if df.filter(F.isnull(F.col("icao24_addr"))).count() > 0:
        print("icao24_addr has NULL values after join.")
        raise BrokenPipeError()


def field_vals_to_nulls(
    df: DataFrame, 
    col_map: dict[str, List]
) -> DataFrame:
    """In case dataframe has different words that represent NULL, reset those 
    words back to NULL."""
    # If field value in word list then retain value, else None
    for col, words in col_map.items():
        bool_expr = ~(F.col(col) == F.col(col))
        for w in words:
            bool_expr |= (F.col(col) == w) # only becomes True when word found
            
        df = df.withColumn(
            col, 
            F.when(~(bool_expr), F.col(col))
        )

    return df


def preprocess_aircrafts(df: DataFrame) -> DataFrame:
    """Drop NULLs & unused columns. Remap NULL-representing values to NULL.
    Specify operator identifier based on priority ICAO -> IATA -> name (if have)
    """
    df = df.drop("manufacturer_name", "operator_callsign", "owner", "note") \
            .dropna("all") \
            .dropna(subset = ["manufacturer_code"])
    df = field_vals_to_nulls(
        df,
        {
            "line_num": ["\tN/A", "-", "n/a"],
            "registration": ["-UNKNOWN-"]
        }
    )

    # Get identifier for operator
    df = df.withColumn(
        "operator_identifier",
        F.when(
            ~F.col("operator_icao").isNull(),
            F.col("operator_icao")
        ).otherwise(
            F.when(
                ~F.col("operator_iata").isNull(),
                F.col("operator_iata")
            )
        )
    )
    
    return df
        

def preprocess_manufacturers(df: DataFrame) -> DataFrame:
    """Skip first line. Rename columns."""
    return df.offset(1) \
        .withColumnsRenamed(
            {
                "Code": "code",
                "Name": "manufacturer"
            }
        )


def preprocess_aircraft_types(df: DataFrame) -> DataFrame:
    """Drop unused columns. Rename columns. Drop duplicate rows."""
    return df.drop("Designator", "ManufacturerCode", "ModelFullName", "WTC") \
        .withColumnsRenamed(
            {
                "AircraftDescription": "aircraft_type",
                "Description": "icao_type_code",
                "EngineCount": "engine_cnt",
                "EngineType": "engine_type"
            }
        ) \
        .drop_duplicates()


def preprocess_airlines(df: DataFrame) -> DataFrame:
    """Rename columns. Unpivot based on airline name."""
    return df.withColumnsRenamed(
        {
            "Name": "operating_airline",
            "Code": "iata",
            "ICAO": "icao"
        }
    ) \
        .melt(
            "operating_airline", 
            values = ["iata", "icao"],
            variableColumnName = "code",
            valueColumnName = "identifier"
        ) \
        .drop("code")


if __name__ == "__main__":
    main()