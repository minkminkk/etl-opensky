"""Utility functions for WebHDFS usage in Airflow DAGs

Please refer to WebHDFS docs for more information.
(https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)
"""

from typing import List, IO
import requests
from time import sleep

from configs import WebHDFSConfig

import logging
logger = logging.getLogger(__name__)


def list_files(path: str) -> List[str]:
    """List filenames in HDFS directory using WebHDFS API"""
    # Send request
    url = f"{WebHDFSConfig().url_prefix}/{path}"
    response = requests.get(url, params = {"op": "LISTSTATUS"})

    # Decode JSON response
    parsed_response = response.json()

    # Check JSON schema
    try:
        files_info = parsed_response["FileStatuses"]["FileStatus"]
        filenames = [file["pathSuffix"] for file in files_info]
    except KeyError:
        if response.status_code >= 500:
            response.raise_for_status()

        exception = parsed_response["RemoteException"]["exception"]
        msg = parsed_response["RemoteException"]["message"]

        logging.error(f"{exception}: {msg}")
        raise Exception("{0}: {1}".format(exception, msg))

    return filenames


def get_write_url(hdfs_path: str, **kwargs) -> str | None:
    """Get URL to write new file from HDFS
    
    Arg:
        hdfs_path [str]: Absolute file path on HDFS.
        **kwargs: for adding additional params on request.
    
    Returns:
        write_url [str]: URL for writing on HDFS.

    Raises:
        HTTPError: Server-side error.
        FileExistsError: When file exists on server but overwriting not allowed.
        Exception: Other exception response from WebHDFS.
    """
    # Initial request to get write location
    get_location_url = WebHDFSConfig().url_prefix + hdfs_path
    print("Sleepingggggggggggggggggg")
    sleep(1)
    response = requests.put(get_location_url, params = kwargs | {"op": "CREATE"})

    # Decode JSON response
    parsed_response = response.json()

    # Check JSON schema
    try:
        write_url = parsed_response["Location"]
    except KeyError:
        if response.status_code >= 500:
            response.raise_for_status()

        exception = parsed_response["RemoteException"]["exception"]
        msg = parsed_response["RemoteException"]["message"]

        logging.error(f"{exception}: {msg}")
        raise Exception("{0}: {1}".format(exception, msg))
    
    return write_url


def upload(file: IO, hdfs_path: str, overwrite: bool = False) -> int:
    """Upload a local file to HDFS.
    
    Arg:
        file [str]: Absolute path of local file to upload.
        hdfs_path [str]: Destination absolute path in HDFS.
        overwrite [bool]: Overwrite file if exists or not.
            Cancel if overwrite is False and file exists.

    Usage:
        with open("path/to/local/file", "rb") as file:
            webhdfs.upload(file, "path/in/hdfs", ...)
    """
    # Get write location
    write_url = get_write_url(
        hdfs_path, 
        overwrite = overwrite, 
        noredirect = True
    )

    # Request to write new file at write location
    response = requests.put(write_url, files = {"name": file})

    if response.status_code != 201:
        parsed_response = response.json()
        exception = parsed_response["RemoteException"]["exception"]
        msg = parsed_response["RemoteException"]["message"]

        if exception == "FileAlreadyExistsException":
            logging.error(f"File already exists while overwriting is disabled.")
            raise FileExistsError("{0}: {1}".format(exception, msg))
        else:
            logging.error(f"{exception}: {msg}")
            raise Exception("{0}: {1}".format(exception, msg))