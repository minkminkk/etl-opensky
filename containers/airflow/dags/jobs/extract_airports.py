import os
import logging
import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.errors.exceptions.captured import AnalysisException

from configs.paths import SPARK_MASTER_URI, HDFS_URI_PREFIX

import requests


def main():
    data_path = "/data_lake/airports.json"
    data_uri = f"{HDFS_URI_PREFIX}/{data_path}"

    spark = SparkSession.builder \
        .master(SPARK_MASTER_URI) \
        .getOrCreate()
    
    try:
        df_airports = spark.read.json(data_uri)
        df_airports.show()
    except AnalysisException:
        with open("/*/airports.json", "r") as file:
            for line in file.readlines():
                print(line)
        # print(file)
        # print(json_dict)

    # Set configuration on Hadoop's config object instead of SparkSession builder
    # because Spark container does not have Hadoop installed, therefore the 
    # Hadoop config when instantiated will be as the default ones. 
    # conf = spark._jsc.hadoopConfiguration()
    # conf.set("fs.defaultFS", "hdfs://namenode:8020/")
    # hadoop = spark._jvm.org.apache.hadoop
    # fs = hadoop.fs.FileSystem
    # data_path_hdfs = hadoop.fs.Path(data_path)

    # # Compare versions between extracted file and data lake file 
    # if fs.exists(data_path_hdfs):
    #     print("Airport data exists in data lake. Moving to next tasks...")
    #     return 0
    # else:
    #     print("Data not available in data lake. Writing data to data lake...")
    #     df_airports = spark.read.json("airports.json")    


def process_response(response: requests.Response):
    # Parse JSON response
    response_dict = response.json()

    # Check JSON schema
    try:
        version = response_dict["version"]
        list_airports = response_dict["rows"]
        
        row_fields = ("name", "country", "iata", "icao", "lat", "lon", "alt")
        for field in row_fields:
            list_airports[0][field]
    except KeyError or TypeError:
        raise Exception("Unexpected response JSON schema")
    
    return (version, list_airports)


if __name__ == "__main__":
    main()