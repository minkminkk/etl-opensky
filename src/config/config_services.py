"""File for DAGs related configs"""

from pyspark.conf import SparkConf
from pyspark.sql.types import *


"""Service addresses and URIs"""
class ServiceConfig:
    """Class for service configs"""
    def __init__(self, key: str):
        lookup = {
            "hdfs": ("hdfs", "namenode", "8020"),
            "webhdfs": ("http", "namenode", "9870"),
            "spark": ("spark", "spark-master", "7077"),
            "hive_metastore": ("thrift", "hive-metastore", "9083")
        }
        try:
            self.service, self.hostname, self.port = lookup[key]
        except KeyError:
            raise ValueError("Invalid input key.")

    @property
    def addr(self):
        return f"{self.hostname}:{self.port}"

    @property
    def uri(self):
        return f"{self.service}://{self.addr}"


"""Spark configs and schemas"""
def get_default_SparkConf() -> SparkConf:
    """Get a SparkConf object with some default values"""
    HDFS_CONF = ServiceConfig("hdfs")
    SPARK_CONF = ServiceConfig("spark")
    HIVE_METASTORE_CONF = ServiceConfig("hive_metastore")

    default_configs = [
        ("spark.master", SPARK_CONF.uri),
        ("spark.sql.warehouse.dir", HDFS_CONF.uri + "/data_warehouse"),
        (
            "spark.hadoop.fs.defaultFS",
            f"{HDFS_CONF.service}://{HDFS_CONF.hostname}"
        ),
        ("spark.hadoop.dfs.replication", 1),
        ("spark.hive.exec.dynamic.partition", True),
        ("spark.hive.exec.dynamic.partition.mode", "nonstrict"),  
        ("spark.hive.metastore.uris", HIVE_METASTORE_CONF.uri)  
            # so that Spark can read metadata into catalog
    ]
        
    conf = SparkConf().setAll(default_configs)
    return conf


class SparkSchema:
    """Contains PySpark schemas used in DAGs"""
    def __init__(self):
        # Source schema expected for flights data from OpenSky API
        self.src_flights = StructType(
            [
                StructField("icao24", StringType()),
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
                StructField("arrivalAirportCandidatesCount", ShortType()),
                # Partition columns
                StructField("flight_year", ShortType()),
                StructField("flight_month", ByteType()),
                StructField("flight_day", ByteType()),
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