from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from configs import get_default_SparkConf


def main() -> None:
    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()
    
    """Create Hive tables"""
    # dim_airports
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dim_airports (
            airport_dim_id INTEGER,
            icao_code CHAR(4) NOT NULL,
            iata_code CHAR(3),
            name VARCHAR(80),
            country VARCHAR(50),
            lat FLOAT,
            lon FLOAT,
            alt SMALLINT
        ) USING hive;
    """)
    
    # dim_dates
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dim_dates (
            date_dim_id INTEGER,
            date_date DATE,
            year SMALLINT,
            month TINYINT,
            day TINYINT,
            week_of_year TINYINT,
            day_of_week TINYINT
        ) USING hive;
    """)

    # dim_aircrafts
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dim_aircrafts (
            aircraft_dim_id INTEGER,
            icao24_addr CHAR(6),
            registration VARCHAR(30),
            operating_airline VARCHAR(80),
            manufacturer VARCHAR(200),
            model VARCHAR(100),
            serial_num VARCHAR(30),
            line_num VARCHAR(30),
            icao_designator VARCHAR(4),
            icao_type CHAR(3),
            aircraft_type VARCHAR(15),
            engine_cnt TINYINT,
            engine_type VARCHAR(20)
        ) USING hive;
    """)

    # fct_flights
    spark.sql("""CREATE TABLE IF NOT EXISTS fct_flights (
            aircraft_dim_id INTEGER,
            depart_date_id INTEGER,
            depart_ts TIMESTAMP,
            depart_airport_dim_id INTEGER,
            arrival_date_id INTEGER,
            arrival_ts TIMESTAMP,
            arrival_airport_dim_id INTEGER
        ) USING hive;
    """)


if __name__ == "__main__":
    main()