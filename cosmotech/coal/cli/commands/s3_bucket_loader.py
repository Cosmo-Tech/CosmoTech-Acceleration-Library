# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib

import boto3

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.utils.logger import LOGGER


def get_connection(is_client=True):
    connect_function = boto3.client if is_client else boto3.resource
    return connect_function('s3')


@click.command()
@click.option("--target-folder",
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              help="The folder in which to download the bucket content",
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--bucket-name",
              envvar="CSM_DATA_BUCKET_NAME",
              help="The bucket on S3 to download",
              metavar="BUCKET",
              type=str,
              show_envvar=True,
              required=True)
def s3_bucket_load_command(target_folder, bucket_name):
    """Download S3 bucket content to a given folder

Make use of the default AWS/S3 configuration to access the bucket

More information is available on this page: 
[https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html)

The following environment variables can be used to configure the connection:
- `AWS_ENDPOINT_URL` : The uri pointing to the S3 service endpoint
- `AWS_ACCESS_KEY_ID` : Your access key to the service
- `AWS_SECRET_ACCESS_KEY` : The secret associated to the access key
"""
    s3_resource = get_connection(False)
    s3_client = get_connection()

    bucket = s3_resource.Bucket(bucket_name)

    pathlib.Path(target_folder).mkdir(parents=True, exist_ok=True)

    for _file in bucket.objects.all():
        LOGGER.info(f"Downloading {_file.key}")
        output_file = f"{target_folder}/{_file.key}"
        s3_client.download_file(bucket_name, _file.key, output_file)
