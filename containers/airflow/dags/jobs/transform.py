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
            "callsign"
        ) \
        .withColumnsRenamed(
            {
                "icao24": "aircraft_icao24",
                "firstSeen": "depart_ts",
                "estDepartureAirport": "depart_airport",
                "lastSeen": "arrival_ts",
                "estArrivalAirport": "arrival_airport",
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
    df_cur_partition.show(10)


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