# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from io import BytesIO
from typing import Optional

import boto3
import pyarrow.csv as pc
import pyarrow.parquet as pq

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER

VALID_TYPES = (
    "sqlite",
    "csv",
    "parquet",
)


@click.command()
@click.option("--store-folder",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              help="The folder containing the store files",
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--output-type",
              default="sqlite",
              help="Choose the type of file output to use",
              type=click.Choice(VALID_TYPES,
                                case_sensitive=False))
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
@web_help("csm-data/store/dump-to-s3")
def dump_to_s3(
    store_folder,
    bucket_name: str,
    endpoint_url: str,
    access_id: str,
    secret_key: str,
    output_type: str,
    file_prefix: str = "",
    use_ssl: bool = True,
    ssl_cert_bundle: Optional[str] = None
):
    """Dump a datastore to a S3

Will upload everything from a given data store to a S3 bucket.

3 modes currently exists :
  - sqlite : will dump the data store underlying database as is
  - csv : will convert every table of the datastore to csv and send them as separate files
  - parquet : will convert every table of the datastore to parquet and send them as separate files

Giving a prefix will add it to every upload (finishing the prefix with a "/" will allow to upload in a folder inside the bucket)

Make use of the boto3 library to access the bucket

More information is available on this page: 
[https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html)
"""
    _s = Store(store_location=store_folder)

    if output_type not in VALID_TYPES:
        LOGGER.error(f"{output_type} is not a valid type of output")
        raise ValueError(f"{output_type} is not a valid type of output")

    boto3_parameters = {
        "use_ssl": use_ssl,
        "endpoint_url": endpoint_url,
        "aws_access_key_id": access_id,
        "aws_secret_access_key": secret_key,
    }
    if ssl_cert_bundle:
        boto3_parameters["verify"] = ssl_cert_bundle

    s3_client = boto3.client("s3", **boto3_parameters)

    def data_upload(data_stream: BytesIO, file_name: str):
        uploaded_file_name = file_prefix + file_name
        data_stream.seek(0)
        size = len(data_stream.read())
        data_stream.seek(0)

        LOGGER.info(f"  Sending {size} bytes of data")
        s3_client.upload_fileobj(data_stream, bucket_name, uploaded_file_name)

    if output_type == "sqlite":
        _file_path = _s._database_path
        _file_name = "db.sqlite"
        _uploaded_file_name = file_prefix + _file_name
        LOGGER.info(f"Sending {_file_path} as {_uploaded_file_name}")
        s3_client.upload_file(_file_path, bucket_name, _uploaded_file_name)
    else:
        tables = list(_s.list_tables())
        for table_name in tables:
            _data_stream = BytesIO()
            _file_name = None
            if output_type == "csv":
                _file_name = table_name + ".csv"
                pc.write_csv(_s.get_table(table_name), _data_stream)
            elif output_type == "parquet":
                _file_name = table_name + ".parquet"
                pq.write_table(_s.get_table(table_name), _data_stream)
            LOGGER.info(f"Sending table {table_name} as {output_type}")
            data_upload(_data_stream, _file_name)
