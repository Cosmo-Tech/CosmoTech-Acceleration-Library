# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from typing import Optional

import boto3

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@click.option(
    "--bucket-name",
    envvar="CSM_DATA_BUCKET_NAME",
    help="The bucket on S3 to delete",
    metavar="BUCKET",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--prefix-filter",
    "file_prefix",
    envvar="CSM_DATA_BUCKET_PREFIX",
    help="A prefix by which all deleted files should start in the bucket",
    metavar="PREFIX",
    type=str,
    show_envvar=True,
)
@click.option(
    "--use-ssl/--no-ssl",
    default=True,
    help="Use SSL to secure connection to S3",
    type=bool,
    is_flag=True,
)
@click.option(
    "--s3-url",
    "endpoint_url",
    help="URL to connect to the S3 system",
    type=str,
    required=True,
    show_envvar=True,
    metavar="URL",
    envvar="AWS_ENDPOINT_URL",
)
@click.option(
    "--access-id",
    "access_id",
    help="Identity used to connect to the S3 system",
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AWS_ACCESS_KEY_ID",
)
@click.option(
    "--secret-key",
    "secret_key",
    help="Secret tied to the ID used to connect to the S3 system",
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AWS_SECRET_ACCESS_KEY",
)
@click.option(
    "--ssl-cert-bundle",
    help="Path to an alternate CA Bundle to validate SSL connections",
    type=str,
    show_envvar=True,
    metavar="PATH",
    envvar="CSM_S3_CA_BUNDLE",
)
@web_help("csm-data/s3-bucket-delete")
def s3_bucket_delete(
    bucket_name: str,
    file_prefix: str,
    endpoint_url: str,
    access_id: str,
    secret_key: str,
    use_ssl: bool = True,
    ssl_cert_bundle: Optional[str] = None,
):
    """Delete S3 bucket content to a given folder

    Will delete everything in the bucket unless a prefix is set, then only file following the given prefix will be deleted

    Make use of the boto3 library to access the bucket

    More information is available on this page:
    [https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html)
    """
    boto3_parameters = {
        "use_ssl": use_ssl,
        "endpoint_url": endpoint_url,
        "aws_access_key_id": access_id,
        "aws_secret_access_key": secret_key,
    }
    if ssl_cert_bundle:
        boto3_parameters["verify"] = ssl_cert_bundle

    s3_resource = boto3.resource("s3", **boto3_parameters)
    bucket = s3_resource.Bucket(bucket_name)

    if file_prefix:
        bucket_files = bucket.objects.filter(Prefix=file_prefix)
    else:
        bucket_files = bucket.objects.all()

    boto_objects = [
        {"Key": _file.key} for _file in bucket_files if _file.key != file_prefix
    ]
    if boto_objects:
        LOGGER.info(f"Deleting {boto_objects}")
        boto_delete_request = {"Objects": boto_objects}
        bucket.delete_objects(Delete=boto_delete_request)
    else:
        LOGGER.info("No objects to delete")
