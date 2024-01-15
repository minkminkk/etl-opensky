"""Utility functions for WebHDFS usage in Airflow DAGs

Please refer to WebHDFS docs for more information.
(https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)
"""

from typing import IO
from configs import WebHDFSConfig
import requests

import logging
logger = logging.getLogger(__name__)


def file_exists(path: str) -> bool:
    """Check if a file in HDFS exists using WebHDFS API
    
    Arg:
        path [str]: Absolute path of HDFS file.

    Returns:
        exists [bool]: File exists or not.
    """
    url = f"{WebHDFSConfig().url_prefix}/{path}"
    logger.debug(f"Sending request to {url}")
    response = requests.get(url, params = {"op": "LISTSTATUS"})
    logger.debug(f"Response status code: {response.status_code}")
    logger.debug(f"Response text: {response.text}")

    # Decode JSON response
    response = response.json()

    # Check decoded JSON schema as dict
    try:
        response["FileStatuses"]["FileStatus"]
    except KeyError: # if can parse into JSON but not above schema -> exception
        logger.info(response["RemoteException"]["message"])
        return False

    return True


def get_write_url(hdfs_path: str, **kwargs) -> str:
    """Get URL to write new file from HDFS
    
    Arg:
        hdfs_path [str]: Absolute file path on HDFS.
        **kwargs: for adding additional params on request.
    
    Returns:
        write_url [str]: URL for writing on HDFS.
    """
    get_location_url = WebHDFSConfig().url_prefix + hdfs_path

    # Initial request to get write location
    response = requests.put(
        get_location_url, 
        params = kwargs | {"op": "CREATE"}
    )

    # Decode JSON response
    parsed_response = response.json()

    # Check JSON schema
    try:
        write_url = parsed_response["Location"]
    except KeyError:
        if response.status_code >= 500:
            response.raise_for_status()
        logging.error(parsed_response["RemoteException"]["exception"])
        logging.error(parsed_response["RemoteException"]["message"])
        raise Exception(
            "{}".format(parsed_response["RemoteException"]["message"])
        )
    
    return write_url


def upload(file: IO, hdfs_path: str, overwrite: bool = False) -> None:
    """Upload a local file to HDFS.
    
    Arg:
        file [str]: Absolute path of local file to upload.
        hdfs_path [str]: Destination absolute path in HDFS.
        overwrite [bool]: Overwrite file if exists or not.
    """
    write_url = get_write_url(hdfs_path, overwrite = overwrite, noredirect = True)

    # Request to write new file at write location
    response = requests.put(write_url, files = {"name": file})

    if response.status_code != 201:
        exception_json = response.json()
        logging.error(response.json()["RemoteException"]["exception"])
        logging.error(response.json()["RemoteException"]["message"])
        raise Exception("{}".format(exception_json["RemoteException"]["message"]))