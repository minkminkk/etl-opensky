import argparse
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

from configs import HDFSConfig, SparkConfig


def main(execution_date: datetime) -> None:
    """Transformation on flights data"""
    # Get configs
    HDFS_CONF = HDFSConfig()
    SPARK_CONF = SparkConfig()
    partition_uri = HDFS_CONF.uri \
        + "/data_lake/flights" \
        + f"/year={execution_date.year}" \
        + f"/month={execution_date.month}" \
        + f"/day={execution_date.day}"

    # Create SparkSession
    spark = SparkSession.builder \
        .master(SPARK_CONF.uri) \
        .config("spark.sql.warehouse.dir", SPARK_CONF.sql_warehouse_dir) \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Read current data
    df_cur_partition = spark.read.parquet(partition_uri)
    
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
    
    # Lookup dim_id in remaining dimensions
    df_airports = spark.sql("SELECT airport_dim_id, icao_code FROM dim_airports;")
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
    
    df_aircrafts = spark.sql(
        "SELECT aircraft_dim_id, icao24_addr FROM dim_aircrafts;"
    )
    df_cur_partition = df_cur_partition \
        .join(
            df_aircrafts,
            on = (df_cur_partition["aircraft_icao24"] \
                == df_aircrafts["icao24_addr"]),
            how = "left"
        ) \
        .drop("icao24_addr")
    
    df_cur_partition.show()


if __name__ == "__main__":
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