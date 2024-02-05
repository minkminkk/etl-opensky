# 
# Note: This job implementation is prone to errors when upstream changes.
# (Different JSON files with more or less rows -> Wrong dim_id for old data)
# Therefore, this is just a temporary implementation with fixed JSON files.
# 

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import *
import pyspark.sql.functions as F

from typing import List
from datetime import datetime
import hdfs
import json

from config_services import ServiceConfig, get_default_SparkConf, SparkSchema
SCHEMAS = SparkSchema()
WEBHDFS_URI = ServiceConfig("webhdfs").uri


def main(data_date: datetime) -> None:
    """Load aircrafts dimension table"""
    # Get configs
    dir_path = "/data_lake"
    
    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()

    # Get aircrafts data from data lake and preprocess
    df_aircrafts = preprocess_aircrafts(
        spark.read.csv(
            dir_path + "/aircraft-database-complete-2024-01.csv", 
            schema = SCHEMAS.src_aircrafts
        )
    )

    # Checking FK constraints with fact data
    print("Checking FK constraint with fact data...")
    df_aircrafts_fk_constraint_check(df_aircrafts, data_date)
    print("Check successful. Continue processing...")
    
    # Preprocess manufacturer data
    df_manufacturers = preprocess_manufacturers(
        spark.read.csv(
            dir_path + "/doc8643Manufacturers.csv",
            schema = SCHEMAS.src_manufacturers
        )
    )
    
    # Preprocess aircraft type data
    df_aircraft_types = preprocess_aircraft_types(
        spark.read.csv(
            dir_path + "/doc8643AircraftTypes.csv",
            schema = SCHEMAS.src_aircraft_types
        )
    )
    
    # Read and preprocess airline data
    hdfs_path = "/data_lake/airlines.json"
    client = hdfs.InsecureClient(WEBHDFS_URI)
    with client.read(hdfs_path) as file:
        airlines = json.load(file)["rows"]
    
    df_airlines = preprocess_airlines(
        spark.createDataFrame(
            airlines, 
            schema = SCHEMAS.src_airlines
        )
    )

    # Augment aircraft data - 
    # Drop columns outside of final DataFrame in the process 
    df_aircrafts = df_aircrafts \
        .join(
            df_manufacturers,
            on = df_aircrafts["manufacturer_code"] \
                == df_manufacturers["code"],
            how = "left"
        ) \
        .drop("manufacturer_code", "code")
        
    df_aircrafts = df_aircrafts \
        .join(
            df_aircraft_types,
            on = df_aircrafts["icao_type"] \
                == df_aircraft_types["icao_type_code"],
            how = "left"
        ) \
        .drop("icao_type_code")
    
    df_aircrafts = df_aircrafts \
        .join(
            df_airlines,
            on = df_aircrafts["operator_identifier"] \
                    == df_airlines["identifier"],  # identifier: ICAO/IATA code
            how = "left"
        ).withColumn(
            "operating_airline",
            F.when(
                df_airlines["identifier"].isNull(), 
                df_aircrafts["operator_name"]  
            ).otherwise(df_airlines["operating_airline"]) 
            # if no such airlines in aircrafts table then NULL
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
    
    # Compare current and new data
    cur_df_aircrafts = spark.table("dim_aircrafts")
    if cur_df_aircrafts == df_aircrafts:
        print("No new data was found. Cancelled writing.")
    else:
        print("New data found. Overwrite on old data.")
        df_aircrafts.show(10)   # for logging purposes
        df_aircrafts.write \
            .mode("overwrite") \
            .format("hive") \
            .saveAsTable("dim_aircrafts")


def field_vals_to_nulls(
    df: DataFrame, 
    col_map: dict[str, List]
) -> DataFrame:
    """In case dataframe has different words that represent NULL, reset those 
    words back to NULL."""
    # If field value in word list then retain value, else None
    for col, words in col_map.items():
        bool_expr = ~(F.col(col) == F.col(col)) # initially False
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
        .dropna(subset = ["manufacturer_code"]) \
        .where("LENGTH(icao_designator) <= 4 OR icao_designator IS NULL") \
        .where("LENGTH(icao_type) == 3 OR icao_type IS NULL")   
            # fields that do not satisfy `where` could be filled with NULLs
            # instead of dropping 
    df = field_vals_to_nulls(
        df,
        {
            "line_num": ["\tN/A", "-", "n/a"],
            "registration": ["-UNKNOWN-"]
        }
    )

    # Get identifier for operator (prio icao -> iata -> NULL)
    df = df.withColumn(
        "operator_identifier",
        F.coalesce("operator_icao", "operator_iata")
    )
    
    return df
        

def preprocess_manufacturers(df: DataFrame) -> DataFrame:
    """Skip first line. Rename columns."""
    return df.offset(1) \
        .withColumnsRenamed({"Code": "code", "Name": "manufacturer"})


def preprocess_aircraft_types(df: DataFrame) -> DataFrame:
    """Select & rename columns. Drop duplicate rows."""
    return df.select(
            "AircraftDescription", 
            "Description", 
            "EngineCount", 
            "EngineType"
        ) \
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
            ids = "operating_airline", 
            values = ["iata", "icao"],
            variableColumnName = "code",
            valueColumnName = "identifier"
        ) \
        .drop("code")


def df_aircrafts_fk_constraint_check(
    df_aircrafts: DataFrame, 
    data_date: datetime
):
    """Check FK constraints before generating dim_id in aircrafts table 
        flights.icao24 (FK) -> aircrafts.icao24_addr (PK)
    """
    spark = SparkSession.getActiveSession()

    flight_data_path = "/data_lake/flights"
    df_flights = spark.read.parquet(flight_data_path) \
        .filter(
            (F.col("flight_year") == data_date.year)
            & (F.col("flight_month") == data_date.month)
            & (F.col("flight_day") == data_date.day)
        ) \
        .select("icao24")
    df_flights = df_flights.join(
        df_aircrafts, 
        on = (df_flights["icao24"] == df_aircrafts["icao24_addr"]),
        how = "left"
    )

    # Check if filter too much so that dim table PK does not exist for fact table
    assert df_flights.filter(F.isnull("icao24_addr")).count() > 0


if __name__ == "__main__":
    import argparse
    import os

    # Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("data_date",
        help = "data date = execution date - 1 day (in YYYY-MM-DD)"
    )
    args = parser.parse_args()

    # Specify timezone as UTC so that strptime can parse date strings into UTC
    # instead of local time (No effect to actual environment variables)
    os.environ["TZ"] = "Europe/London"

    # Preliminary input validation
    try:
        args.data_date = datetime.strptime(args.data_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Invalid input date.")

    # Call main function
    main(data_date = args.data_date)