from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

from datetime import datetime
from config_services import get_default_SparkConf


def main(data_date: datetime) -> None:
    """Transformation on flights data"""
    # Get configs
    data_path = "/data_lake/flights"
    data_date_str = data_date.strftime("%Y-%m-%d")
    print(f"Transforming and loading fct_flights table on {data_date}...")

    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Read flights data partition
    df_flights = spark.read.parquet(data_path) \
        .filter(
            (F.col("flight_year") == data_date.year)
            & (F.col("flight_month") == data_date.month)
            & (F.col("flight_day") == data_date.day)
        )
    
    # Filter & rename columns
    df_flights = df_flights \
        .withColumnsRenamed(
            {
                "icao24": "aircraft_icao24",
                "firstSeen": "depart_ts",
                "estDepartureAirport": "depart_airport_icao",
                "lastSeen": "arrival_ts",
                "estArrivalAirport": "arrival_airport_icao",
            }
        )
    
    # Calculate timestamps and date_dim_id
    df_flights = df_flights \
        .withColumns(
            {
                "depart_ts": F.timestamp_seconds("depart_ts"),
                "arrival_ts": F.timestamp_seconds("arrival_ts"),
                "flight_date_dim_id": \
                    F.col("flight_year") * 10**4 \
                    + F.col("flight_month") * 10**2 \
                    + F.col("flight_day")
            }
        ).drop("flight_year", "flight_month", "flight_day")
    
    # Join dimensions to get dim_id for the remaining dim columns in fact table
    # dim_airports
    df_airports = spark \
        .table("dim_airports") \
        .select("airport_dim_id", "icao_code")
    df_flights = df_flights \
        .join(
            df_airports,
            on = (df_flights["depart_airport_icao"] \
                == df_airports["icao_code"]),
            how = "left"
        ) \
        .withColumnRenamed("airport_dim_id", "depart_airport_dim_id") \
        .drop("depart_airport_icao", "icao_code") \
        .join(
            df_airports,
            on = (df_flights["arrival_airport_icao"] \
                == df_airports["icao_code"]),
            how = "left"
        ) \
        .withColumnRenamed("airport_dim_id", "arrival_airport_dim_id") \
        .drop("arrival_airport_icao", "icao_code")
    
    # dim_aircrafts
    df_aircrafts = spark \
        .table("dim_aircrafts") \
        .select("aircraft_dim_id", "icao24_addr")
    df_flights = df_flights \
        .join(
            df_aircrafts,
            on = (df_flights["aircraft_icao24"] \
                == df_aircrafts["icao24_addr"]),
            how = "left"
        ) \
        .drop("aircraft_icao24", "icao24_addr")

    # Reorder columns because df.subtract() is based on column position not names
    df_flights = df_flights.select(
        "aircraft_dim_id", 
        "depart_ts", 
        "depart_airport_dim_id", 
        "arrival_ts", 
        "arrival_airport_dim_id", 
        "flight_date_dim_id"
    )

    # Compare current and processing data. Append new data.
    cur_df_flights = spark.table("fct_flights") \
        .filter(F.col("flight_date_dim_id") == data_date.strftime("%Y%m%d"))
    df_append = df_flights.subtract(cur_df_flights)

    if df_append.isEmpty():
        print(f"No new flights data on {data_date_str}.")
        return
    else:
        df_append.limit(10).show()
        df_append.write \
            .mode("append") \
            .format("hive") \
            .partitionBy("flight_date_dim_id") \
            .saveAsTable("fct_flights")


if __name__ == "__main__":
    import argparse
    import os

    # Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("data_date",
        help = "date of data (in YYYY-MM-DD)"
    )
    args = parser.parse_args()

    # Specify timezone as UTC so that strptime can parse date strings into UTC
    # instead of local time (No effect to actual environment variables)
    os.environ["TZ"] = "Europe/London"

    # Preliminary input validation
    try:
        args.data_date = datetime.strptime(args.data_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("\"data_date\" must be in YYYY-MM-DD format.")

    # Call main function
    main(data_date = args.data_date)