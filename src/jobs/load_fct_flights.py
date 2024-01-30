from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

from datetime import datetime
from configs import get_default_SparkConf


def main(execution_date: datetime) -> None:
    """Transformation on flights data"""
    # Get configs
    partition_path = "/data_lake/flights" \
        + f"/depart_year={execution_date.year}" \
        + f"/depart_month={execution_date.month}" \
        + f"/depart_day={execution_date.day}"

    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Read current data
    df_cur_partition = spark.read.parquet(partition_path)
    
    # Filter & rename columns
    df_cur_partition = df_cur_partition \
        .select(
            "icao24",
            "firstSeen",
            "estDepartureAirport",
            "lastSeen",
            "estArrivalAirport",
        ) \
        .withColumnsRenamed(
            {
                "icao24": "aircraft_icao24",
                "firstSeen": "depart_ts",
                "estDepartureAirport": "depart_airport_icao",
                "lastSeen": "arrival_ts",
                "estArrivalAirport": "arrival_airport_icao",
            }
        )
    
    # Calculate dates and timestamps
    df_cur_partition = df_cur_partition \
        .dropna(subset = "depart_ts") \
        .withColumns(
            {
                "depart_ts": F.timestamp_seconds(df_cur_partition["depart_ts"]),
                "arrival_ts": F.timestamp_seconds(df_cur_partition["arrival_ts"]),
                "depart_dim_date_id": F.to_date(
                    df_cur_partition["depart_ts"], "yyyyMMdd"
                ),
                "arrival_dim_date_id": F.to_date(
                    df_cur_partition["arrival_ts"], "yyyyMMdd"
                )
            }
        )
    
    # Join dimensions to get dim_id for dim columns in fact table
    df_airports = spark \
        .table("dim_airports") \
        .select("airport_dim_id", "icao_code")
    df_cur_partition = df_cur_partition \
        .join(
            df_airports,
            on = (df_cur_partition["depart_airport_icao"] \
                == df_airports["icao_code"]),
            how = "left"
        ) \
        .withColumnRenamed("airport_dim_id", "depart_airport_dim_id") \
        .drop("icao_code") \
        .join(
            df_airports,
            on = (df_cur_partition["arrival_airport_icao"] \
                == df_airports["icao_code"]),
            how = "left"
        ) \
        .drop("icao_code")
    
    df_aircrafts = spark \
        .table("dim_aircrafts") \
        .select("aircraft_dim_id", "icao24_addr")
    df_cur_partition = df_cur_partition \
        .join(
            df_aircrafts,
            on = (df_cur_partition["aircraft_icao24"] \
                == df_aircrafts["icao24_addr"]),
            how = "left"
        ) \
        .drop("icao24_addr")
    
    df_cur_partition.limit(10).show()


if __name__ == "__main__":
    import argparse
    import os

    # Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("execution_date",
        help = "date of data (in YYYY-MM-DD)"
    )
    args = parser.parse_args()

    # Specify timezone as UTC so that strptime can parse date strings into UTC
    # instead of local time (No effect to actual environment variables)
    os.environ["TZ"] = "Europe/London"

    # Preliminary input validation
    try:
        args.execution_date = datetime.strptime(args.execution_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("\"execution_date\" must be in YYYY-MM-DD format.")

    # Call main function
    main(execution_date = args.execution_date)