# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib
from typing import Optional

import boto3

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help, translate_help
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


@click.command()
@click.option("--target-folder",
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              help=T("coal-help.commands.storage.s3_bucket_download.parameters.target_folder"),
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--bucket-name",
              envvar="CSM_DATA_BUCKET_NAME",
              help=T("coal-help.commands.storage.s3_bucket_download.parameters.bucket_name"),
              metavar="BUCKET",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--prefix-filter",
              "file_prefix",
              envvar="CSM_DATA_BUCKET_PREFIX",
              help=T("coal-help.commands.storage.s3_bucket_download.parameters.prefix_filter"),
              metavar="PREFIX",
              type=str,
              show_envvar=True)
@click.option("--use-ssl/--no-ssl",
              default=True,
              help=T("coal-help.commands.storage.s3_bucket_download.parameters.use_ssl"),
              type=bool,
              is_flag=True)
@click.option("--s3-url",
              "endpoint_url",
              help=T("coal-help.commands.storage.s3_bucket_download.parameters.s3_url"),
              type=str,
              required=True,
              show_envvar=True,
              metavar="URL",
              envvar="AWS_ENDPOINT_URL")
@click.option("--access-id",
              "access_id",
              help=T("coal-help.commands.storage.s3_bucket_download.parameters.access_id"),
              type=str,
              required=True,
              show_envvar=True,
              metavar="ID",
              envvar="AWS_ACCESS_KEY_ID")
@click.option("--secret-key",
              "secret_key",
              help=T("coal-help.commands.storage.s3_bucket_download.parameters.secret_key"),
              type=str,
              required=True,
              show_envvar=True,
              metavar="ID",
              envvar="AWS_SECRET_ACCESS_KEY")
@click.option("--ssl-cert-bundle",
              help=T("coal-help.commands.storage.s3_bucket_download.parameters.ssl_cert_bundle"),
              type=str,
              show_envvar=True,
              metavar="PATH",
              envvar="CSM_S3_CA_BUNDLE")
@web_help("csm-data/s3-bucket-download")
@translate_help("coal-help.commands.storage.s3_bucket_download.description")
def s3_bucket_download(
    target_folder: str,
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

    s3_resource = boto3.resource("s3",
                                 **boto3_parameters)

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
            pathlib.Path(output_file).parent.mkdir(parents=True,exist_ok=True)
            LOGGER.info(T("coal.logs.storage.downloading").format(path=path_name, output=output_file))
            bucket.download_file(_file.key, output_file)
