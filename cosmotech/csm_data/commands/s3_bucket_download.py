# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from typing import Optional

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import web_help, translate_help
from cosmotech.orchestrator.utils.translate import T


@click.command()
@click.option(
    "--target-folder",
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    help=T("csm-data.commands.storage.s3_bucket_download.parameters.target_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--bucket-name",
    envvar="CSM_DATA_BUCKET_NAME",
    help=T("csm-data.commands.storage.s3_bucket_download.parameters.bucket_name"),
    metavar="BUCKET",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--prefix-filter",
    "file_prefix",
    envvar="CSM_DATA_BUCKET_PREFIX",
    help=T("csm-data.commands.storage.s3_bucket_download.parameters.prefix_filter"),
    metavar="PREFIX",
    type=str,
    show_envvar=True,
)
@click.option(
    "--use-ssl/--no-ssl",
    default=True,
    help=T("csm-data.commands.storage.s3_bucket_download.parameters.use_ssl"),
    type=bool,
    is_flag=True,
)
@click.option(
    "--s3-url",
    "endpoint_url",
    help=T("csm-data.commands.storage.s3_bucket_download.parameters.s3_url"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="URL",
    envvar="AWS_ENDPOINT_URL",
)
@click.option(
    "--access-id",
    "access_id",
    help=T("csm-data.commands.storage.s3_bucket_download.parameters.access_id"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AWS_ACCESS_KEY_ID",
)
@click.option(
    "--secret-key",
    "secret_key",
    help=T("csm-data.commands.storage.s3_bucket_download.parameters.secret_key"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AWS_SECRET_ACCESS_KEY",
)
@click.option(
    "--ssl-cert-bundle",
    help=T("csm-data.commands.storage.s3_bucket_download.parameters.ssl_cert_bundle"),
    type=str,
    show_envvar=True,
    metavar="PATH",
    envvar="CSM_S3_CA_BUNDLE",
)
@web_help("csm-data/s3-bucket-download")
@translate_help("csm-data.commands.storage.s3_bucket_download.description")
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
    # Import the functions at the start of the command
    from cosmotech.coal.aws.s3 import create_s3_resource, download_files

    # Create S3 resource
    s3_resource = create_s3_resource(
        endpoint_url=endpoint_url,
        access_id=access_id,
        secret_key=secret_key,
        use_ssl=use_ssl,
        ssl_cert_bundle=ssl_cert_bundle,
    )

    # Download files
    download_files(
        target_folder=target_folder,
        bucket_name=bucket_name,
        s3_resource=s3_resource,
        file_prefix=file_prefix,
    )
