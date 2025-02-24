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
@click.option("--source-folder",
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              help=T("coal-help.commands.storage.s3_bucket_upload.parameters.source_folder"),
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--recursive/--no-recursive",
              default=False,
              help=T("coal-help.commands.storage.s3_bucket_upload.parameters.recursive"),
              type=bool,
              is_flag=True)
@click.option("--bucket-name",
              envvar="CSM_DATA_BUCKET_NAME",
              help=T("coal-help.commands.storage.s3_bucket_upload.parameters.bucket_name"),
              metavar="BUCKET",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--prefix",
              "file_prefix",
              envvar="CSM_DATA_BUCKET_PREFIX",
              help=T("coal-help.commands.storage.s3_bucket_upload.parameters.prefix"),
              metavar="PREFIX",
              type=str,
              show_envvar=True,
              default="")
@click.option("--use-ssl/--no-ssl",
              default=True,
              help=T("coal-help.commands.storage.s3_bucket_upload.parameters.use_ssl"),
              type=bool,
              is_flag=True)
@click.option("--s3-url",
              "endpoint_url",
              help=T("coal-help.commands.storage.s3_bucket_upload.parameters.s3_url"),
              type=str,
              required=True,
              show_envvar=True,
              metavar="URL",
              envvar="AWS_ENDPOINT_URL")
@click.option("--access-id",
              "access_id",
              help=T("coal-help.commands.storage.s3_bucket_upload.parameters.access_id"),
              type=str,
              required=True,
              show_envvar=True,
              metavar="ID",
              envvar="AWS_ACCESS_KEY_ID")
@click.option("--secret-key",
              "secret_key",
              help=T("coal-help.commands.storage.s3_bucket_upload.parameters.secret_key"),
              type=str,
              required=True,
              show_envvar=True,
              metavar="ID",
              envvar="AWS_SECRET_ACCESS_KEY")
@click.option("--ssl-cert-bundle",
              help=T("coal-help.commands.storage.s3_bucket_upload.parameters.ssl_cert_bundle"),
              type=str,
              show_envvar=True,
              metavar="PATH",
              envvar="CSM_S3_CA_BUNDLE")
@web_help("csm-data/s3-bucket-upload")
@translate_help("coal-help.commands.storage.s3_bucket_upload.description")
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
    source_path = pathlib.Path(source_folder)
    if not source_path.exists():
        LOGGER.error(T("coal.errors.file_system.file_not_found").format(source_folder=source_folder))
        raise FileNotFoundError(T("coal.errors.file_system.file_not_found").format(source_folder=source_folder))

    boto3_parameters = {
        "use_ssl": use_ssl,
        "endpoint_url": endpoint_url,
        "aws_access_key_id": access_id,
        "aws_secret_access_key": secret_key,
    }
    if ssl_cert_bundle:
        boto3_parameters["verify"] = ssl_cert_bundle

    s3_resource = boto3.resource("s3", **boto3_parameters)

    def file_upload(file_path: pathlib.Path, file_name: str):
        uploaded_file_name = file_prefix + file_name
        LOGGER.info(T("coal.logs.data_transfer.file_sent").format(file_path=file_path, uploaded_name=uploaded_file_name))
        s3_resource.Bucket(bucket_name).upload_file(file_path, uploaded_file_name)

    if source_path.is_dir():
        _source_name = str(source_path)
        for _file_path in source_path.glob("**/*" if recursive else "*"):
            if _file_path.is_file():
                _file_name = str(_file_path).removeprefix(_source_name).removeprefix("/")
                file_upload(_file_path, _file_name)
    else:
        file_upload(source_path, source_path.name)
