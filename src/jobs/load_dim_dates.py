import os
import logging
import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import lit, date_format, \
    year, month, day, weekofyear, dayofweek, \
    explode, sequence, to_date 
from pyspark.errors.exceptions.captured import AnalysisException

DATE_FORMAT = "%Y-%m-%d"

def main(start_date: str, end_date: str):
    spark = SparkSession.builder \
        .appName("Load dates dimension table") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/data_warehouse") \
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
    df_dates = populate_date_df(spark, start_date, end_date)

    # Compare current data with generated date data - skip task if identical
    df_append = df_dates.subtract(df_cur)
    if df_append.isEmpty():
        logging.info(f"Data has already loaded. Ending...")
        return 0
    
    # Write into DWH
    df_append.show()        # for logging added data
    df_append.write \
        .mode("append") \
        .format("hive") \
        .saveAsTable("dim_dates")
        

def populate_date_df(
    spark: SparkSession, 
    start_date: str, 
    end_date: str
):
    """Populate calendar date from start_date to end_date.

    Args:
        spark [pyspark.sql.SparkSession]: SparkSession object.
        start_date [str]: Start date to populate (YYYY-MM-DD).
        end_date [str]: End date to populate (YYYY-MM-DD).

    Returns:
        df_dates [pyspark.sql.DataFrame]: Calendar date DataFrame.
    """
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
        datetime.strptime(args.start_date, DATE_FORMAT)
        datetime.strptime(args.end_date, DATE_FORMAT)
    except ValueError:
        raise ValueError("\"execution_date\" must be in YYYY-MM-DD format.")

    # Call main function
    main(start_date = args.start_date, end_date = args.end_date)