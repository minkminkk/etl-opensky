-- dim_airports --
CREATE TABLE IF NOT EXISTS dim_airports (
    airport_dim_id INTEGER,
    icao_code CHAR(4) NOT NULL,
    iata_code CHAR(3),
    name VARCHAR(80),
    country VARCHAR(50),
    lat FLOAT,
    lon FLOAT,
    alt SMALLINT,
    PRIMARY KEY (airport_dim_id)
);


-- dim_dates --
CREATE TABLE IF NOT EXISTS dim_dates (
    date_dim_id INTEGER,
    date DATE,
    year SMALLINT,
    month TINYINT,
    day TINYINT,
    week_of_year TINYINT,
    day_of_week TINYINT
    PRIMARY KEY (date_dim_id)
);


-- dim_aircrafts --
CREATE TABLE IF NOT EXISTS dim_aircrafts (
    aircraft_dim_id INTEGER,
    icao24_addr CHAR(6) NOT NULL,
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
    engine_type VARCHAR(20),
    PRIMARY KEY (aircraft_dim_id)
);