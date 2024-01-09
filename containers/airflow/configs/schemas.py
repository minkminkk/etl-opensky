"""Contains schemas used in DAGs"""

from pyspark.sql.types import StructType, StructField, \
    ShortType, IntegerType, LongType, StringType


SRC_FLIGHTS_SCHEMA = StructType([
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
])