import os
import logging
import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, to_timestamp, year, month, day
from pyspark.errors.exceptions.captured import AnalysisException

from configs.general import DATE_FORMAT, AIRPORT_ICAO
from configs.schemas import SRC_FLIGHTS_SCHEMA
from configs.paths import SPARK_MASTER_URI, HDFS_URI_PREFIX

import requests


def main(
    airport: str,
    execution_date: datetime
):
    """Extract data from OpenSky API, rename columns, and write data into HDFS.
    Partition by departureDate.

    Required command-line args:
        airport: 4-letter ICAO24 address of airport (case-insensitive).
        execution_date [YYYY-MM-DD]: Date of data.
    """
    data_path = "data_lake/flights"
    data_uri = f"{HDFS_URI_PREFIX}/{data_path}"
    partition_uri = data_uri \
        + f"/year={execution_date.year}" \
        + f"/month={execution_date.month}" \
        + f"/day={execution_date.day}"
    
    spark = SparkSession.builder \
        .master(SPARK_MASTER_URI) \
        .getOrCreate()

    # Read current data in partition to a DataFrame
    try:
        df_cur_partition = spark.read.parquet(partition_uri) \
            .drop("year", "month", "day")
    except AnalysisException:
        df_cur_partition = spark.createDataFrame([], schema = SRC_FLIGHTS_SCHEMA)

    # Extract data into another DataFrame
    df_extract = spark.createDataFrame([], schema = SRC_FLIGHTS_SCHEMA)
    for type in ["departure", "arrival"]:
        response = request_opensky(type, airport, execution_date)
        list_flights = process_response(response)

        df = spark.createDataFrame(list_flights, schema = SRC_FLIGHTS_SCHEMA)
        df_extract = df_extract.unionByName(df)
    
    # Compare current data with generated date data - skip task if identical
    df_append = df_extract.subtract(df_cur_partition)

    # If no data to append then skip to end of task 
    if df_append.isEmpty():
        logging.info(f"No new flights data for {execution_date.date}. Ending...")
        return 0
    
    # Create partition
    df_append = df_append.withColumn(
        "departure_ts", 
        to_timestamp(from_unixtime(df_append["firstSeen"]))
    )
    # Split into another line to avoid conflict on firstSeen
    df_append = df_append \
        .withColumn("year", year(df_append["departure_ts"])) \
        .withColumn("month", month(df_append["departure_ts"])) \
        .withColumn("day", day(df_append["departure_ts"])) \
        .drop("departure_ts")
    
    # Write into HDFS
    df_append.show()     # for logging added data
    df_append.write \
        .partitionBy("year", "month", "day") \
        .parquet(data_uri, mode = "append")


def request_opensky(
    type: str,
    airport_icao: str,
    execution_date: datetime
):
    """Send requests to OpenSky API and return response.

    Args:
        type [str]: Literal values 'departure' or 'arrival', indicating type of
            flight to/from an airport.
        airport_icao [str]: ICAO code of airport (4 letters, case insensitive).
        execution_date [datetime]: Date of data.

    Returns:
        response [requests.Response]: Response from server 
            (checked for 4xx, 5xx code).

    Raises:
        HTTPError: Response contains 4xx, 5xx status code.
    """
    # Input validation
    if type not in ("arrival", "departure"):
        raise ValueError("\"type\" must be \"arrival\" or \"departure\".")

    # Warning if requested date range is larger than API limit
    if execution_date > datetime.today():
        raise ValueError("Cannot get data from the future")

    # Extract data from API
    start_ts = int(execution_date.timestamp())
    end_ts = int((execution_date + timedelta(days = 1)).timestamp())

    url = f"https://opensky-network.org/api/flights/{type}"

    logging.info(
        f"Extracting {type} data in {execution_date.strftime(DATE_FORMAT)}"
    )
    response = requests.get(
        url, params = {"airport": airport_icao, "begin": start_ts, "end": end_ts}
    )

    # Check for 4xx, 5xx status code
    response.raise_for_status()

    return response
    

def process_response(response: requests.Response):
    """Process response (evaluate status code, parse response)

    Arg:
        response [requests.Response]: Response from server.

    Returns:
        list_flights [List[dict]]: Data about extracted flight. 
            Generate 1 list per successful API call.

    Raises:
        Exception: When the response does not contain the expected JSON schema.
    """
    # Parse JSON response
    list_flights = response.json() # Into list of dicts

    # Check response schema
    try:
        for name in SRC_FLIGHTS_SCHEMA.fieldNames():
            list_flights[0][name]
    except KeyError or TypeError:
        logging.error("Unexcepted response schema.")
        raise Exception("Unexpected response schema.")

    return list_flights
        

if __name__ == "__main__":
    # Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("airport",
        help = "airport to be investigated, " \
            + "input as ICAO24 address (4 case-insensitive hexadecimal letters)"
    )
    parser.add_argument("execution_date",
        help = "date of data (in YYYY-MM-DD)"
    )
    args = parser.parse_args()

    # Specify timezone as UTC so that strptime can parse date strings into UTC
    # instead of local time (No effect to actual environment variables)
    os.environ["TZ"] = "Europe/London"

    # Preliminary input validation
    if len(args.airport) != 4:
        raise ValueError("\"airport\" must be 4 letters.")
    
    try:
        args.execution_date = datetime.strptime(args.execution_date, DATE_FORMAT)
    except ValueError:
        raise ValueError("\"execution_date\" must be in YYYY-MM-DD format.")

    # Call main function
    main(
        airport = args.airport,
        execution_date = args.execution_date
    )