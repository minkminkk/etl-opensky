"""Utility functions for WebHDFS usage in Airflow DAGs

Please refer to WebHDFS docs for more information.
(https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)
"""

from configs.paths import *
import requests
import logging


def file_exists(path: str):
    """Check if a file in HDFS exists using WebHDFS API
    
    Arg:
        path [str]: Absolute path of HDFS file.

    Returns:
        exists [bool]: File exists or not.
    """
    url = f"{WEBHDFS_URL_PREFIX}/{path}"
    logging.debug(f"Sending request to {url}")
    response = requests.get(url, params = {"op": "LISTSTATUS"})
    logging.debug(f"Response status code: {response.status_code}")
    logging.debug(f"Response text: {response.text}")

    # Decode JSON response
    response = response.json()

    # Check decoded JSON schema as dict
    try:
        response["FileStatuses"]["FileStatus"]
    except KeyError: # if can parse into JSON but not above schema -> exception
        logging.info(response["RemoteException"]["message"])
        return False

    return True


def create_file(local_path: str, hdfs_path: str, overwrite: bool):
    """Create a file in HDFS from local file.
        file(s) in HDFS."
    
    Arg:
        local_path [str]: Absolute path of file to upload.
        hdfs_path [str]: Destination absolute path in HDFS.
        overwrite [bool]: Overwrite existing file or not.

    Returns:
        exists [bool]: File exists or not.
    """
    get_location_url = f"{WEBHDFS_URL_PREFIX}/{hdfs_path}"
    
    # Initial request to get write location
    logging.debug(f"Sending request to {get_location_url}")
    response = requests.put(
        get_location_url, 
        params = {"op": "CREATE", "overwrite": overwrite, "nodirect": True}
    )
    logging.debug(f"Response status code: {response.status_code}")
    logging.debug(f"Response text: {response.text}")
    
    # Decode JSON response
    response = response.json()

    try:
        write_url = response["Location"]
    except KeyError:
        raise requests.exceptions.InvalidJSONError("Invalid response JSON schema")

    # Request to write new file at write location
    with open(local_path, "r") as file:
        logging.debug(f"Sending request to {write_url}")
        response = requests.put(write_url, files = {"name": file})

    if response.status_code != 201:
        raise Exception("Could not write file to HDFS.")