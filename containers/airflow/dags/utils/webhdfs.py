"""Utility functions for WebHDFS usage in Airflow DAGs

Please refer to WebHDFS docs for more information.
(https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)
"""
from typing import List, IO
import requests
import logging

from airflow.exceptions import AirflowSkipException

from configs import WebHDFSConfig


def list_file_sizes(path: str) -> List[str]:
    """List files and their sizes in bytes in HDFS directory using WebHDFS API
    (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#List_a_Directory)
    """
    
    # Send request
    url = WebHDFSConfig().url_prefix + path
    response = requests.get(url, params = {"op": "LISTSTATUS"})

    # Decode JSON response
    parsed_response = response.json()

    # Check JSON schema
    try:
        file_infos = parsed_response["FileStatuses"]["FileStatus"]
        file_sizes = {}
        for file in file_infos:
            file_sizes[file["pathSuffix"]] = file["length"]

    except KeyError:
        if response.status_code >= 500:
            response.raise_for_status()

        exception = parsed_response["RemoteException"]["exception"]
        msg = parsed_response["RemoteException"]["message"]

        logging.error(f"{exception}: {msg}")
        raise Exception("{0}: {1}".format(exception, msg))

    return file_sizes


def get_io_url(operation: str, hdfs_path: str, **kwargs) -> str:
    """Get URL to read/write file from HDFS in case noredirect flag is True
    
    Arg:
        hdfs_path [str]: Absolute file path on HDFS.
        kwargs: optional request params (overwrite, noredirect...)
    
    Returns:
        [str] URL for reading/writing on HDFS.

    Raises:
        HTTPError: Server-side error.
        Exception: Other exception response from WebHDFS.
    """
    # URL to get reading/writing URL
    location_url = WebHDFSConfig().url_prefix + hdfs_path
    
    # Make request
    if operation == "read":
        params = kwargs | {"op": "OPEN"}
        response = requests.get(location_url, params = params)
    elif operation == "write":
        params = kwargs | {"op": "CREATE"}
        response = requests.put(location_url, params = params)
    else:
        logging.error("Operation must be \"read\" or \"write\".")
        raise ValueError("Invalid operation.")

    # Error catching
    if response.status_code >= 500:
        response.raise_for_status()
    elif 400 <= response.status_code < 500:
        parsed_response = response.json()
        exception = parsed_response["RemoteException"]["exception"]
        msg = parsed_response["RemoteException"]["message"]

        if exception == "FileAlreadyExistsException":
            # If noredirect flag is False then can be raised here
            logging.warning(f"File already exists while overwriting is disabled.")
            raise FileExistsError("{0}: {1}".format(exception, msg))
        else:
            logging.error(f"{exception}: {msg}")
            raise Exception("{0}: {1}".format(exception, msg))

    # Response processing
    noredirect = kwargs.get("noredirect", False)
    if noredirect is True:
        parsed_response = response.json()
        return parsed_response["Location"]
    else:
        return response.url


def read(hdfs_path: str, **kwargs) -> IO:
    """Open a remote file in HDFS using WebHDFS.
    (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Open_and_Read_a_File)
    
    Arg:
        hdfs_path [str]: Destination absolute path in HDFS.
        kwargs: optional request params (overwrite, noredirect...)

    Returns:
    #######

    Raises:
        HTTPError: Server-side error.
        Exception: Other exception response from WebHDFS.

    Usage:
        with open("path/to/local/file", "rb") as file:
            webhdfs.upload(file, "path/in/hdfs", ...)
    """
    # Logic based on noredirect flag
    file_url = WebHDFSConfig().url_prefix + hdfs_path
    noredirect = kwargs.get("noredirect", False)

    if noredirect is True:
        response = requests.get(file_url, params = kwargs | {"OP": "OPEN"})
    else:
        read_url = get_io_url("read", hdfs_path, noredirect = noredirect)
        response = requests.get(read_url)

    # Error catching
    if response.status_code != 200:
        if response.status_code >= 500:
            response.raise_for_status()

        parsed_response = response.json()
        exception = parsed_response["RemoteException"]["exception"]
        msg = parsed_response["RemoteException"]["message"]

        if exception == "FileAlreadyExistsException":
            # If noredirect flag is True then can be raised here
            logging.warning(f"File already exists while overwriting is disabled.")
            raise FileExistsError("{0}: {1}".format(exception, msg))
        else:
            logging.error(f"{exception}: {msg}")
            raise Exception("{0}: {1}".format(exception, msg))
        
    # Response processing
    # Response payload in read requests typically contains metadata section 
    # at the start, therefore strip it to get initial file content
    return response.text \
        .split("\r\n\r\n", maxsplit = 1)[1] \
        .rsplit("\r\n", maxsplit = 2)[0]


def upload(file: IO, hdfs_path: str, **kwargs) -> None:
    """Upload a local file to HDFS using WebHDFS.
    (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Create_and_Write_to_a_File)
    
    Arg:
        file [str]: Absolute path of local file to upload.
        hdfs_path [str]: Destination absolute path in HDFS.
        kwargs: optional request params (overwrite, noredirect...)

    Raises:
        HTTPError: Server-side error.
        FileExistsError: When file exists on server but overwriting not allowed.
        Exception: Other exception response from WebHDFS.

    Usage:
        with open("path/to/local/file", "rb") as file:
            webhdfs.upload(file, "path/in/hdfs", ...)
    """
    # Logic based on noredirect flag
    file_url = WebHDFSConfig().url_prefix + hdfs_path
    noredirect = kwargs.get("noredirect", False)

    if noredirect is True:
        response = requests.put(
            file_url,
            params = kwargs | {"op": "CREATE"}, 
            files = {"name": file}
        )
    else:
        write_url = get_io_url(
            "write",
            hdfs_path, 
            overwrite = kwargs.get("overwrite", False),
            noredirect = noredirect,
        )
        response = requests.put(write_url, files = {"name": file})

    # Error catching
    if response.status_code != 201:
        if response.status_code >= 500:
            response.raise_for_status()

        parsed_response = response.json()
        exception = parsed_response["RemoteException"]["exception"]
        msg = parsed_response["RemoteException"]["message"]

        if exception == "FileAlreadyExistsException":
            logging.warning(f"File already exists while overwriting is disabled.")
            raise FileExistsError("{0}: {1}".format(exception, msg))
        else:
            logging.error(f"{exception}: {msg}")
            raise Exception("{0}: {1}".format(exception, msg))