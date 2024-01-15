import os
import logging
import argparse
from datetime import datetime, timedelta

from configs import SparkConfig

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.errors.exceptions.captured import AnalysisException


def main(start_date: str, end_date: str) -> None:
    # Get configs
    spark_conf = SparkConfig()

    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Load dates dimension table") \
        .master(spark_conf.uri) \
        .config("spark.sql.warehouse.dir", spark_conf.sql_warehouse_dir) \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Create table if not exists
    # (https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)
    spark.sql("""CREATE TABLE IF NOT EXISTS dim_dates (
        date_dim_id INT,
        date DATE,
        year SMALLINT,
        month TINYINT,
        day TINYINT,
        week_of_year TINYINT,
        day_of_week TINYINT
    ) USING hive;""")

    # Get current data in date table
    df_cur = spark.sql("SELECT * FROM dim_dates;")

    # Populate date data 
    df_dates = populate_date_df(start_date, end_date)

    # Compare current data with generated date data - skip task if identical
    df_append = df_dates.subtract(df_cur)
    if df_append.isEmpty():
        logging.info(f"Data has already loaded. Ending...")
        return
    
    # Write into DWH
    df_append.show()        # for logging added data
    df_append.write \
        .mode("append") \
        .format("hive") \
        .saveAsTable("dim_dates")
        

def populate_date_df(start_date: str, end_date: str) -> DataFrame:
    """Populate calendar date from start_date to end_date"""
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
        ) AS date;
    """) \
        .createOrReplaceTempView("dates")
    df_dates = spark.sql(f"""
        SELECT 
            ((YEAR(date) * 10000) + MONTH(date) * 100 + DAY(date)) AS date_dim_id,
            date, 
            YEAR(date) AS year,
            MONTH(date) AS month,
            DAY(date) AS day,
            WEEKOFYEAR(date) AS week_of_year,
            DAYOFWEEK(date) AS day_of_week
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
        logging.error("\"execution_date\" must be in YYYY-MM-DD format.")
        raise

    # Call main function
    main(start_date = args.start_date, end_date = args.end_date)