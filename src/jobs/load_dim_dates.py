import argparse
from datetime import datetime
import os
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *

from configs import SparkConfig


def main(start_date: str, end_date: str) -> None:
    # Get configs
    spark_conf = SparkConfig()

    # Create SparkSession
    spark = SparkSession.builder \
        .master(spark_conf.uri) \
        .config("spark.sql.warehouse.dir", spark_conf.sql_warehouse_dir) \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Get current data in date table
    df_cur = spark.sql(f"SELECT * FROM dim_dates WHERE \
        date_date BETWEEN '{start_date}' AND '{end_date}';")
    expected_num_rows = spark.sql(
        f"SELECT DATE_DIFF('{end_date}', '{start_date}') + 1 AS cnt;"
    ).collect()[0]["cnt"]

    print(df_cur.count(), expected_num_rows)
    # Compare current data with expected - skip task if already have
    if df_cur.count() == expected_num_rows:
        print(f"Data has already fully loaded. Ending...")
        return "skipped"

    # Populate date data 
    df_dates = populate_date_df(start_date, end_date)

    # Data to append
    df_append = df_dates.subtract(df_cur)
    df_append.show(10)        # for logging added data
    
    # Write into DWH
    df_append.write \
        .mode("append") \
        .format("hive") \
        .saveAsTable("dim_dates")
        

def populate_date_df(start_date: str, end_date: str) -> DataFrame:
    """Populate calendar date from start_date to end_date
    
    Schema of generated DataFrame:
    |--- date_dim_id        (IntegerType())
    |--- date_date          (DateType())
    |--- year               (ShortType())
    |--- month              (ByteType())
    |--- day                (ByteType())
    |--- week_of_year       (ByteType())
    |--- day_of_week        (ByteType())
    """
    spark = SparkSession.getActiveSession()

    # Reference
    # (https://3cloudsolutions.com/resources/generate-a-calendar-dimension-in-spark/)
    spark.sql(f"""
        SELECT EXPLODE(
            SEQUENCE(
                TO_DATE('{start_date}'), 
                TO_DATE('{end_date}'), 
                INTERVAL 1 day
            )
        ) AS date_date;
    """) \
        .createOrReplaceTempView("dates")
    df_dates = spark.sql(f"""
        SELECT 
            (
                (YEAR(date_date) * 10000) 
                + MONTH(date_date) * 100 
                + DAY(date_date)
            ) AS date_dim_id,
            date_date, 
            YEAR(date_date) AS year,
            MONTH(date_date) AS month,
            DAY(date_date) AS day,
            WEEKOFYEAR(date_date) AS week_of_year,
            DAYOFWEEK(date_date) AS day_of_week
        FROM dates;
    """)

    return df_dates


if __name__ == "__main__":
    # Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date",
        help = "date of data (in YYYY-MM-DD)"
    )
    parser.add_argument("end_date",
        help = "date of data (in YYYY-MM-DD)"
    )
    args = parser.parse_args()

    # Specify timezone as UTC so that strptime can parse date strings into UTC
    # instead of local time (No effect to actual environment variables)
    os.environ["TZ"] = "Europe/London"

    # Preliminary input validation
    try:
        datetime.strptime(args.start_date, "%Y-%m-%d")
        datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        logging.error("\"start_date\", \"end_date\" must be YYYY-MM-DD.")
        raise ValueError("Invalid input dates.")

    # Call main function
    main(start_date = args.start_date, end_date = args.end_date)