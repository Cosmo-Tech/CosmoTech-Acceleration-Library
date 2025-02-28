# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from typing import Optional

import boto3

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help, translate_help
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


@click.command()
@click.option(
    "--bucket-name",
    envvar="CSM_DATA_BUCKET_NAME",
    help=T("coal-help.commands.storage.s3_bucket_delete.parameters.bucket_name"),
    metavar="BUCKET",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--prefix-filter",
    "file_prefix",
    envvar="CSM_DATA_BUCKET_PREFIX",
    help=T("coal-help.commands.storage.s3_bucket_delete.parameters.prefix_filter"),
    metavar="PREFIX",
    type=str,
    show_envvar=True,
)
@click.option(
    "--use-ssl/--no-ssl",
    default=True,
    help=T("coal-help.commands.storage.s3_bucket_delete.parameters.use_ssl"),
    type=bool,
    is_flag=True,
)
@click.option(
    "--s3-url",
    "endpoint_url",
    help=T("coal-help.commands.storage.s3_bucket_delete.parameters.s3_url"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="URL",
    envvar="AWS_ENDPOINT_URL",
)
@click.option(
    "--access-id",
    "access_id",
    help=T("coal-help.commands.storage.s3_bucket_delete.parameters.access_id"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AWS_ACCESS_KEY_ID",
)
@click.option(
    "--secret-key",
    "secret_key",
    help=T("coal-help.commands.storage.s3_bucket_delete.parameters.secret_key"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AWS_SECRET_ACCESS_KEY",
)
@click.option(
    "--ssl-cert-bundle",
    help=T("coal-help.commands.storage.s3_bucket_delete.parameters.ssl_cert_bundle"),
    type=str,
    show_envvar=True,
    metavar="PATH",
    envvar="CSM_S3_CA_BUNDLE",
)
@web_help("csm-data/s3-bucket-delete")
@translate_help("coal-help.commands.storage.s3_bucket_delete.description")
def s3_bucket_delete(
    bucket_name: str,
    file_prefix: str,
    endpoint_url: str,
    access_id: str,
    secret_key: str,
    use_ssl: bool = True,
    ssl_cert_bundle: Optional[str] = None,
):
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

    boto_objects = [{"Key": _file.key} for _file in bucket_files if _file.key != file_prefix]
    if boto_objects:
        LOGGER.info(T("coal.logs.storage.deleting_objects").format(objects=boto_objects))
        boto_delete_request = {"Objects": boto_objects}
        bucket.delete_objects(Delete=boto_delete_request)
    else:
        LOGGER.info(T("coal.logs.storage.no_objects"))
