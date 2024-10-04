# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib
from typing import Optional

import boto3

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@click.option("--source-folder",
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              help="The folder/file to upload to the target bucket",
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--recursive/--no-recursive",
              default=False,
              help="Recursively send the content of every folder inside the starting folder to the bucket",
              type=bool,
              is_flag=True)
@click.option("--bucket-name",
              envvar="CSM_DATA_BUCKET_NAME",
              help="The bucket on S3 to upload to",
              metavar="BUCKET",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--prefix",
              "file_prefix",
              envvar="CSM_DATA_BUCKET_PREFIX",
              help="A prefix by which all uploaded files should start with in the bucket",
              metavar="PREFIX",
              type=str,
              show_envvar=True,
              default="")
@click.option("--use-ssl/--no-ssl",
              default=True,
              help="Use SSL to secure connection to S3",
              type=bool,
              is_flag=True)
@click.option("--s3-url",
              "endpoint_url",
              help="URL to connect to the S3 system",
              type=str,
              required=True,
              show_envvar=True,
              metavar="URL",
              envvar="AWS_ENDPOINT_URL")
@click.option("--access-id",
              "access_id",
              help="Identity used to connect to the S3 system",
              type=str,
              required=True,
              show_envvar=True,
              metavar="ID",
              envvar="AWS_ACCESS_KEY_ID")
@click.option("--secret-key",
              "secret_key",
              help="Secret tied to the ID used to connect to the S3 system",
              type=str,
              required=True,
              show_envvar=True,
              metavar="ID",
              envvar="AWS_SECRET_ACCESS_KEY")
@click.option("--ssl-cert-bundle",
              help="Path to an alternate CA Bundle to validate SSL connections",
              type=str,
              show_envvar=True,
              metavar="PATH",
              envvar="CSM_S3_CA_BUNDLE")
@web_help("csm-data/s3-bucket-upload")
def s3_bucket_upload(
    source_folder,
    bucket_name: str,
    endpoint_url: str,
    access_id: str,
    secret_key: str,
    file_prefix: str = "",
    use_ssl: bool = True,
    ssl_cert_bundle: Optional[str] = None,
    recursive: bool = False
):
    """Upload a folder to a S3 Bucket

Will upload everything from a given folder to a S3 bucket. If a single file is passed only it will be uploaded, and recursive will be ignored

Giving a prefix will add it to every upload (finishing the prefix with a "/" will allow to upload in a folder inside the bucket)

Make use of the boto3 library to access the bucket

More information is available on this page: 
[https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html)
"""
    source_path = pathlib.Path(source_folder)
    if not source_path.exists():
        LOGGER.error(f"{source_folder} does not exists")
        raise FileNotFoundError(f"{source_folder} does not exists")

    boto3_parameters = {
        "use_ssl": use_ssl,
        "endpoint_url": endpoint_url,
        "aws_access_key_id": access_id,
        "aws_secret_access_key": secret_key,
    }
    if ssl_cert_bundle:
        boto3_parameters["verify"] = ssl_cert_bundle

    s3_client = boto3.client("s3", **boto3_parameters)

    def file_upload(file_path: pathlib.Path, file_name: str):
        uploaded_file_name = file_prefix + file_name
        LOGGER.info(f"Sending {file_path} as {uploaded_file_name}")
        s3_client.upload_file(file_path, bucket_name, uploaded_file_name)

    if source_path.is_dir():
        _source_name = str(source_path)
        for _file_path in source_path.glob("**/*" if recursive else "*"):
            if _file_path.is_file():
                _file_name = str(_file_path).removeprefix(_source_name).removeprefix("/")
                file_upload(_file_path, _file_name)
    else:
        file_upload(source_path, source_path.name)
