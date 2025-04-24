# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
S3 bucket operations module.

This module provides functions for interacting with S3 buckets, including
uploading, downloading, and deleting files.
"""

import pathlib
from io import BytesIO
from typing import Optional, Dict, Any, List, Iterator

import boto3

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def create_s3_client(
    endpoint_url: str,
    access_id: str,
    secret_key: str,
    use_ssl: bool = True,
    ssl_cert_bundle: Optional[str] = None,
) -> boto3.client:
    """
    Create an S3 client with the given credentials and configuration.

    Args:
        endpoint_url: The S3 endpoint URL
        access_id: The AWS access key ID
        secret_key: The AWS secret access key
        use_ssl: Whether to use SSL for the connection
        ssl_cert_bundle: Path to the SSL certificate bundle

    Returns:
        An S3 client object
    """
    boto3_parameters = {
        "use_ssl": use_ssl,
        "endpoint_url": endpoint_url,
        "aws_access_key_id": access_id,
        "aws_secret_access_key": secret_key,
    }
    if ssl_cert_bundle:
        boto3_parameters["verify"] = ssl_cert_bundle

    return boto3.client("s3", **boto3_parameters)


def create_s3_resource(
    endpoint_url: str,
    access_id: str,
    secret_key: str,
    use_ssl: bool = True,
    ssl_cert_bundle: Optional[str] = None,
) -> boto3.resource:
    """
    Create an S3 resource with the given credentials and configuration.

    Args:
        endpoint_url: The S3 endpoint URL
        access_id: The AWS access key ID
        secret_key: The AWS secret access key
        use_ssl: Whether to use SSL for the connection
        ssl_cert_bundle: Path to the SSL certificate bundle

    Returns:
        An S3 resource object
    """
    boto3_parameters = {
        "use_ssl": use_ssl,
        "endpoint_url": endpoint_url,
        "aws_access_key_id": access_id,
        "aws_secret_access_key": secret_key,
    }
    if ssl_cert_bundle:
        boto3_parameters["verify"] = ssl_cert_bundle

    return boto3.resource("s3", **boto3_parameters)


def upload_file(
    file_path: pathlib.Path,
    bucket_name: str,
    s3_resource: boto3.resource,
    file_prefix: str = "",
) -> None:
    """
    Upload a single file to an S3 bucket.

    Args:
        file_path: Path to the file to upload
        bucket_name: Name of the S3 bucket
        s3_resource: S3 resource object
        file_prefix: Prefix to add to the file name in the bucket
    """
    uploaded_file_name = file_prefix + file_path.name
    LOGGER.info(T("coal.common.data_transfer.file_sent").format(file_path=file_path, uploaded_name=uploaded_file_name))
    s3_resource.Bucket(bucket_name).upload_file(str(file_path), uploaded_file_name)


def upload_folder(
    source_folder: str,
    bucket_name: str,
    s3_resource: boto3.resource,
    file_prefix: str = "",
    recursive: bool = False,
) -> None:
    """
    Upload files from a folder to an S3 bucket.

    Args:
        source_folder: Path to the folder containing files to upload
        bucket_name: Name of the S3 bucket
        s3_resource: S3 resource object
        file_prefix: Prefix to add to the file names in the bucket
        recursive: Whether to recursively upload files from subdirectories
    """
    source_path = pathlib.Path(source_folder)
    if not source_path.exists():
        LOGGER.error(T("coal.common.file_operations.not_found").format(source_folder=source_folder))
        raise FileNotFoundError(T("coal.common.file_operations.not_found").format(source_folder=source_folder))

    if source_path.is_dir():
        _source_name = str(source_path)
        for _file_path in source_path.glob("**/*" if recursive else "*"):
            if _file_path.is_file():
                _file_name = str(_file_path).removeprefix(_source_name).removeprefix("/")
                uploaded_file_name = file_prefix + _file_name
                LOGGER.info(
                    T("coal.common.data_transfer.file_sent").format(
                        file_path=_file_path, uploaded_name=uploaded_file_name
                    )
                )
                s3_resource.Bucket(bucket_name).upload_file(str(_file_path), uploaded_file_name)
    else:
        upload_file(source_path, bucket_name, s3_resource, file_prefix)


def download_files(
    target_folder: str,
    bucket_name: str,
    s3_resource: boto3.resource,
    file_prefix: Optional[str] = None,
) -> None:
    """
    Download files from an S3 bucket to a local folder.

    Args:
        target_folder: Local folder to download files to
        bucket_name: Name of the S3 bucket
        s3_resource: S3 resource object
        file_prefix: Optional prefix to filter objects to download
    """
    bucket = s3_resource.Bucket(bucket_name)

    pathlib.Path(target_folder).mkdir(parents=True, exist_ok=True)
    remove_prefix = False
    if file_prefix:
        bucket_files = bucket.objects.filter(Prefix=file_prefix)
        if file_prefix.endswith("/"):
            remove_prefix = True
    else:
        bucket_files = bucket.objects.all()
    for _file in bucket_files:
        if not (path_name := str(_file.key)).endswith("/"):
            target_file = path_name
            if remove_prefix:
                target_file = target_file.removeprefix(file_prefix)
            output_file = f"{target_folder}/{target_file}"
            pathlib.Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            LOGGER.info(T("coal.services.azure_storage.downloading").format(path=path_name, output=output_file))
            bucket.download_file(_file.key, output_file)


def upload_data_stream(
    data_stream: BytesIO,
    bucket_name: str,
    s3_client: boto3.client,
    file_name: str,
    file_prefix: str = "",
) -> None:
    """
    Upload a data stream to an S3 bucket.

    Args:
        data_stream: BytesIO stream containing the data to upload
        bucket_name: Name of the S3 bucket
        s3_client: S3 client object
        file_name: Name of the file to create in the bucket
        file_prefix: Prefix to add to the file name in the bucket
    """
    uploaded_file_name = file_prefix + file_name
    data_stream.seek(0)
    size = len(data_stream.read())
    data_stream.seek(0)

    LOGGER.info(T("coal.common.data_transfer.sending_data").format(size=size))
    s3_client.upload_fileobj(data_stream, bucket_name, uploaded_file_name)


def delete_objects(
    bucket_name: str,
    s3_resource: boto3.resource,
    file_prefix: Optional[str] = None,
) -> None:
    """
    Delete objects from an S3 bucket, optionally filtered by prefix.

    Args:
        bucket_name: Name of the S3 bucket
        s3_resource: S3 resource object
        file_prefix: Optional prefix to filter objects to delete
    """
    bucket = s3_resource.Bucket(bucket_name)

    if file_prefix:
        bucket_files = bucket.objects.filter(Prefix=file_prefix)
    else:
        bucket_files = bucket.objects.all()

    boto_objects = [{"Key": _file.key} for _file in bucket_files if _file.key != file_prefix]
    if boto_objects:
        LOGGER.info(T("coal.services.azure_storage.deleting_objects").format(objects=boto_objects))
        boto_delete_request = {"Objects": boto_objects}
        bucket.delete_objects(Delete=boto_delete_request)
    else:
        LOGGER.info(T("coal.services.azure_storage.no_objects"))
