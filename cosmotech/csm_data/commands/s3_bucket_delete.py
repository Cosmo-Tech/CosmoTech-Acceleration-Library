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
    "--bucket-name",
    envvar="CSM_DATA_BUCKET_NAME",
    help=T("csm-data.commands.storage.s3_bucket_delete.parameters.bucket_name"),
    metavar="BUCKET",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--prefix-filter",
    "file_prefix",
    envvar="CSM_DATA_BUCKET_PREFIX",
    help=T("csm-data.commands.storage.s3_bucket_delete.parameters.prefix_filter"),
    metavar="PREFIX",
    type=str,
    show_envvar=True,
)
@click.option(
    "--use-ssl/--no-ssl",
    default=True,
    help=T("csm-data.commands.storage.s3_bucket_delete.parameters.use_ssl"),
    type=bool,
    is_flag=True,
)
@click.option(
    "--s3-url",
    "endpoint_url",
    help=T("csm-data.commands.storage.s3_bucket_delete.parameters.s3_url"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="URL",
    envvar="AWS_ENDPOINT_URL",
)
@click.option(
    "--access-id",
    "access_id",
    help=T("csm-data.commands.storage.s3_bucket_delete.parameters.access_id"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AWS_ACCESS_KEY_ID",
)
@click.option(
    "--secret-key",
    "secret_key",
    help=T("csm-data.commands.storage.s3_bucket_delete.parameters.secret_key"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AWS_SECRET_ACCESS_KEY",
)
@click.option(
    "--ssl-cert-bundle",
    help=T("csm-data.commands.storage.s3_bucket_delete.parameters.ssl_cert_bundle"),
    type=str,
    show_envvar=True,
    metavar="PATH",
    envvar="CSM_S3_CA_BUNDLE",
)
@web_help("csm-data/s3-bucket-delete")
@translate_help("csm-data.commands.storage.s3_bucket_delete.description")
def s3_bucket_delete(
    bucket_name: str,
    file_prefix: str,
    endpoint_url: str,
    access_id: str,
    secret_key: str,
    use_ssl: bool = True,
    ssl_cert_bundle: Optional[str] = None,
):
    # Import the functions at the start of the command
    from cosmotech.coal.aws.s3 import create_s3_resource, delete_objects

    # Create S3 resource
    s3_resource = create_s3_resource(
        endpoint_url=endpoint_url,
        access_id=access_id,
        secret_key=secret_key,
        use_ssl=use_ssl,
        ssl_cert_bundle=ssl_cert_bundle,
    )

    # Delete objects
    delete_objects(
        bucket_name=bucket_name,
        s3_resource=s3_resource,
        file_prefix=file_prefix,
    )
