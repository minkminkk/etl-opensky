# 
# Note: This job implementation is prone to errors when upstream changes.
# (Different JSON files with more or less rows -> Wrong dim_id for old data)
# Therefore, this is just a temporary implementation with fixed JSON files.
# 

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
import pyspark.sql.functions as F

from configs import WebHDFSConfig, SparkConfig
import hdfs
import json


def main() -> None:
    """Load airports dimension table"""
    # Get configs
    SPARK_CONF = SparkConfig()
    WEBHDFS_CONF = WebHDFSConfig()
    hdfs_path = "/data_lake/airports.json"
    
    # Create SparkSession
    spark = SparkSession.builder \
        .master(SPARK_CONF.uri) \
        .config("spark.sql.warehouse.dir", SPARK_CONF.sql_warehouse_dir) \
        .enableHiveSupport() \
        .getOrCreate()

    # Get airports data from data lake
    client = hdfs.InsecureClient(WEBHDFS_CONF.uri)
    with client.read(hdfs_path) as file:
        airports = json.load(file)["rows"]
    
    # Some fields are formatted as int although it was expected to be float
    # Therefore, Spark was unable to load DataFrame and write to DWH.
    # Schema reading on creating DataFrame was also tried but did not work as
    # Field values are still integers (e.g. 7 instead of 7.0).
    # Decided to cast fields before loading into DataFrame.
    for airport in airports:
        airport["lat"] = float(airport["lat"])
        airport["lon"] = float(airport["lon"])
        airport["alt"] = int(airport["alt"]) if airport["alt"] != "-1" else None

    # Load airports data to DataFrame
    df_airports = spark.createDataFrame(
        airports, 
        schema = SPARK_CONF.schema.src_airports
    )

    # Add airport_dim_id column and recast unusual columns
    df_airports = df_airports \
        .withColumnsRenamed({"icao": "icao_code", "iata": "iata_code"}) \
        .withColumn("airport_dim_id", F.row_number().over(Window.orderBy("name")))

    # Compare current and processed data to detect new data
    cur_df_airports = spark.sql("SELECT * FROM dim_airports;")
    if cur_df_airports == df_airports:
        print("No new data was detected.")
        return "skipped"
    else:
        print("Detected new data. Overwriting old data")

        # Write to DWH if there is new data
        df_airports.show(10)
        df_airports.write \
            .mode("overwrite") \
            .format("hive") \
            .saveAsTable("dim_airports")


if __name__ == "__main__":
    main()