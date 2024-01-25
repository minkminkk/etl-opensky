"""File for DAGs related configs"""

import os
from pyspark.sql.types import *


class GeneralConfig:
    def __init__(self, airport_icao = "EDDF"):
        self.airport_icao = airport_icao
        self.date_format = "%Y-%m-%d"   # Wildcards based on datetime module

    @property
    def airport_icao(self):
        return self._airport_icao

    @airport_icao.setter
    def airport_icao(self, value: str):
        if not value.isalpha() or len(value) != 4:
            raise ValueError("Airport ICAO code must have 4 letters")
        
        self._airport_icao = value


class ServiceConfig:
    """Base class for service configs"""
    def __init__(self, service: str, hostname: str, port: int):
        self.service = service
        self.hostname = hostname
        self.port = port
    
    @property
    def addr(self):
        return f"{self.hostname}:{self.port}"

    @property
    def uri(self):
        return f"{self.service}://{self.addr}"


"""Specific classes for configs of services"""
class HDFSConfig(ServiceConfig):
    def __init__(self):
        super().__init__("hdfs", "namenode", "8020")


class WebHDFSConfig(ServiceConfig):
    def __init__(self):
        super().__init__("http", "namenode", "9870")

    @property
    def url_prefix(self):
        return f"{self.uri}/webhdfs/v1"
        # Might have to change if webhdfs change versioning


class SparkSchema:
    """Contains PySpark schemas used in DAGs"""
    def __init__(self):
        # Source schema expected for flights data from OpenSky API
        self.src_flights = StructType(
            [
                StructField("icao24", StringType(), nullable = False),
                StructField("firstSeen", LongType()),
                StructField("estDepartureAirport", StringType()),
                StructField("lastSeen", LongType()),
                StructField("estArrivalAirport", StringType()),
                StructField("callsign", StringType()),
                StructField("estDepartureAirportHorizDistance", IntegerType()),
                StructField("estDepartureAirportVertDistance", IntegerType()),
                StructField("estArrivalAirportHorizDistance", IntegerType()),
                StructField("estArrivalAirportVertDistance", IntegerType()),
                StructField("departureAirportCandidatesCount", ShortType()),
                StructField("arrivalAirportCandidatesCount", ShortType())
            ]
        )
        # Source schema expected from local files
        self.src_airports = StructType(
            [
                StructField("name", StringType()),
                StructField("iata", StringType()),
                StructField("icao", StringType()),
                StructField("country", StringType()),
                StructField("lat", FloatType()),
                StructField("lon", FloatType()),
                StructField("alt", ShortType())
            ]
        )
        self.src_airlines = StructType(
            [
                StructField("Name", StringType()),
                StructField("Code", StringType()),
                StructField("ICAO", StringType())
            ]
        )
        self.src_aircrafts = StructType(
            [
                StructField("icao24_addr", StringType()),
                StructField("registration", StringType()),
                StructField("manufacturer_code", StringType()),
                StructField("manufacturer_name", StringType()),
                StructField("model", StringType()),
                StructField("icao_designator", StringType()),
                StructField("serial_num", StringType()),
                StructField("line_num", StringType()),
                StructField("icao_type", StringType()),
                StructField("operator_name", StringType()),
                StructField("operator_callsign", StringType()),
                StructField("operator_icao", StringType()),
                StructField("operator_iata", StringType()),
                StructField("owner", StringType()),
                StructField("note", StringType())
            ]
        )
        self.src_aircraft_types = StructType(
            [
                StructField("AircraftDescription", StringType()),
                StructField("Description", StringType()),
                StructField("Designator", StringType()),
                StructField("EngineCount", ByteType()),
                StructField("EngineType", StringType()),
                StructField("ManufacturerCode", StringType()),
                StructField("ModelFullName", StringType()),
                StructField("WTC", StringType())
            ]
        )
        self.src_manufacturers = StructType(
            [
                StructField("Code", StringType()),
                StructField("Name", StringType())
            ]
        )


class SparkConfig(ServiceConfig):
    def __init__(self):
        super().__init__("spark", "spark-master", 7077)
        self.schema = SparkSchema()

        # Default warehouse dir: ${HDFS_URI}/data_warehouse    
        self.sql_warehouse_dir = f"{HDFSConfig().uri}/data_warehouse"


"""Airflow is also a service. However, its configs specified in ServiceConfig 
class were not used in the DAGs implementation, but its paths instead.

Therefore, AirflowConfig class is not inherited from the ServiceConfig class.
Instead, it is implemented as a standalone class with different attributes.
"""
class AirflowPath:
    def __init__(self):
        self.home = os.getenv("AIRFLOW_HOME")

    @property
    def dags(self):
        return os.path.join(self.home, "dags")
    
    @property
    def jobs(self):
        return os.path.join(self.home, "jobs")
    
    @property
    def config(self):
        return os.path.join(self.home, "config")
    

class AirflowConfig:
    def __init__(self):
        self.path = AirflowPath()