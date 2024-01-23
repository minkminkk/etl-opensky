-- dim_airports --
CREATE EXTERNAL TABLE IF NOT EXISTS dim_airports (
    airport_dim_id INTEGER PRIMARY KEY DISABLE NOVALIDATE,
    icao_code CHAR(4) NOT NULL DISABLE,
    iata_code CHAR(3),
    name VARCHAR(80),
    country VARCHAR(50),
    lat FLOAT,
    lon FLOAT,
    alt SMALLINT
)
LOCATION 'hdfs://namenode:8020/data_warehouse';


-- dim_dates --
CREATE EXTERNAL TABLE IF NOT EXISTS dim_dates (
    date_dim_id INTEGER PRIMARY KEY DISABLE NOVALIDATE,
    date_date DATE,
    year SMALLINT,
    month TINYINT,
    day TINYINT,
    week_of_year TINYINT,
    day_of_week TINYINT
)
LOCATION 'hdfs://namenode:8020/data_warehouse';


-- dim_aircrafts --
CREATE EXTERNAL TABLE IF NOT EXISTS dim_aircrafts (
    aircraft_dim_id INTEGER PRIMARY KEY DISABLE NOVALIDATE,
    icao24_addr CHAR(6) NOT NULL DISABLE,
    registration VARCHAR(10),
    operating_airline VARCHAR(80),
    manufacturer VARCHAR(200),
    model VARCHAR(100),
    serial_num VARCHAR(5),
    line_num VARCHAR(5),
    icao_designator VARCHAR(4),
    icao_type CHAR(3),
    aircraft_type VARCHAR(15),
    engine_cnt TINYINT,
    engine_type VARCHAR(20)
)
LOCATION 'hdfs://namenode:8020/data_warehouse';

-- fct_flights --
CREATE EXTERNAL TABLE IF NOT EXISTS fct_flights (
    aircraft_dim_id INTEGER REFERENCES dim_aircrafts(aircraft_dim_id) DISABLE NOVALIDATE,
    depart_date_id INTEGER REFERENCES dim_dates(date_dim_id) DISABLE NOVALIDATE,
    depart_ts TIMESTAMP,
    depart_airport_dim_id INTEGER REFERENCES dim_airports(airport_dim_id) DISABLE NOVALIDATE,
    arrival_date_id INTEGER REFERENCES dim_dates(date_dim_id) DISABLE NOVALIDATE,
    arrival_ts TIMESTAMP,
    arrival_airport_dim_id INTEGER REFERENCES dim_airports(airport_dim_id) DISABLE NOVALIDATE
)
LOCATION 'hdfs://namenode:8020/data_warehouse';