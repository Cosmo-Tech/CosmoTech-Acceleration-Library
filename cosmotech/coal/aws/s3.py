# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
s3 bucket operations module.

this module provides functions for interacting with S3 buckets, including
uploading, downloading, and deleting files.
"""

import pathlib
from io import BytesIO

import boto3
from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.utils.configuration import Configuration
from cosmotech.coal.utils.logger import LOGGER


class S3:

    def __init__(self, configuration: Configuration):
        self._configuration = configuration.s3

    @property
    def file_prefix(self):
        if "bucket_prefix" in self._configuration:
            return self._configuration.bucket_prefix
        return ""

    @property
    def use_ssl(self):
        if "use_ssl" in self._configuration:
            return self._configuration.use_ssl
        return True

    @property
    def ssl_cert_bundle(self):
        if "ssl_cert_bundle" in self._configuration:
            return self._configuration.ssl_cert_bundle
        return None

    @property
    def access_key_id(self):
        return self._configuration.access_key_id

    @property
    def endpoint_url(self):
        return self._configuration.endpoint_url

    @property
    def bucket_name(self):
        return self._configuration.bucket_name

    @property
    def secret_access_key(self):
        return self._configuration.secret_access_key

    @property
    def output_type(self):
        if "output_type" in self._configuration:
            return self._configuration.output_type
        return "csv"

    @property
    def client(self) -> boto3.client:
        boto3_parameters = {
            "use_ssl": self.use_ssl,
            "endpoint_url": self.endpoint_url,
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
        }
        if self.ssl_cert_bundle:
            boto3_parameters["verify"] = self.ssl_cert_bundle

        return boto3.client("s3", **boto3_parameters)

    @property
    def resource(self) -> boto3.resource:
        boto3_parameters = {
            "use_ssl": self.use_ssl,
            "endpoint_url": self.endpoint_url,
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
        }
        if self.ssl_cert_bundle:
            boto3_parameters["verify"] = self.ssl_cert_bundle

        return boto3.resource("s3", **boto3_parameters)

    def upload_file(self, file_path: pathlib.Path) -> None:
        """
        Upload a single file to an S3 bucket.

        Args:
            file_path: Path to the file to upload
        """
        uploaded_file_name = self.file_prefix + file_path.name
        LOGGER.info(
            T("coal.common.data_transfer.file_sent").format(file_path=file_path, uploaded_name=uploaded_file_name)
        )
        self.resource.Bucket(self.bucket_name).upload_file(str(file_path), uploaded_file_name)

    def upload_folder(self, source_folder: str, recursive: bool = False) -> None:
        """
        Upload files from a folder to an S3 bucket.

        Args:
            source_folder: Path to the folder containing files to upload
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
                    uploaded_file_name = self.file_prefix + _file_name
                    LOGGER.info(
                        T("coal.common.data_transfer.file_sent").format(
                            file_path=_file_path, uploaded_name=uploaded_file_name
                        )
                    )
                    self.resource.Bucket(self.bucket_name).upload_file(str(_file_path), uploaded_file_name)
        else:
            self.upload_file(source_path)

    def download_files(self, destination_folder: str) -> None:
        """
        Download files from an S3 bucket to a local folder.

        Args:
            destination_folder: Local folder to download files to
        """
        bucket = self.resource.Bucket(self.bucket_name)

        pathlib.Path(destination_folder).mkdir(parents=True, exist_ok=True)
        remove_prefix = False
        if self.file_prefix:
            bucket_files = bucket.objects.filter(Prefix=self.file_prefix)
            if self.file_prefix.endswith("/"):
                remove_prefix = True
        else:
            bucket_files = bucket.objects.all()
        for _file in bucket_files:
            if not (path_name := str(_file.key)).endswith("/"):
                target_file = path_name
                if remove_prefix:
                    target_file = target_file.removeprefix(self.file_prefix)
                output_file = f"{destination_folder}/{target_file}"
                pathlib.Path(output_file).parent.mkdir(parents=True, exist_ok=True)
                LOGGER.info(T("coal.services.azure_storage.downloading").format(path=path_name, output=output_file))
                bucket.download_file(_file.key, output_file)

    def upload_data_stream(self, data_stream: BytesIO, file_name: str) -> None:
        """
        Upload a data stream to an S3 bucket.

        Args:
            data_stream: BytesIO stream containing the data to upload
            file_name: Name of the file to create in the bucket
        """
        uploaded_file_name = self.file_prefix + file_name
        data_stream.seek(0)
        size = len(data_stream.read())
        data_stream.seek(0)

        LOGGER.info(T("coal.common.data_transfer.sending_data").format(size=size))
        self.client.upload_fileobj(data_stream, self.bucket_name, uploaded_file_name)

    def delete_objects(self) -> None:
        """
        Delete objects from an S3 bucket, optionally filtered by prefix.

        Args:
            bucket_name: Name of the S3 bucket
            s3_resource: S3 resource object
            file_prefix: Optional prefix to filter objects to delete
        """
        bucket = self.resource.Bucket(self.bucket_name)

        if self.file_prefix:
            bucket_files = bucket.objects.filter(Prefix=self.file_prefix)
        else:
            bucket_files = bucket.objects.all()

        boto_objects = [{"Key": _file.key} for _file in bucket_files if _file.key != self.file_prefix]
        if boto_objects:
            LOGGER.info(T("coal.services.azure_storage.deleting_objects").format(objects=boto_objects))
            boto_delete_request = {"Objects": boto_objects}
            bucket.delete_objects(Delete=boto_delete_request)
        else:
            LOGGER.info(T("coal.services.azure_storage.no_objects"))
