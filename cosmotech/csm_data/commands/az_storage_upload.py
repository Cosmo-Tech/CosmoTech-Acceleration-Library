# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import web_help, translate_help
from cosmotech.orchestrator.utils.translate import T


@click.command()
@click.option(
    "--source-folder",
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    help=T("csm-data.commands.storage.az_storage_upload.parameters.source_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--recursive/--no-recursive",
    default=False,
    help=T("csm-data.commands.storage.az_storage_upload.parameters.recursive"),
    type=bool,
    is_flag=True,
)
@click.option(
    "--blob-name",
    envvar="AZURE_STORAGE_BLOB_NAME",
    help=T("csm-data.commands.storage.az_storage_upload.parameters.blob_name"),
    metavar="BUCKET",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--prefix",
    "file_prefix",
    envvar="CSM_DATA_BLOB_PREFIX",
    help=T("csm-data.commands.storage.az_storage_upload.parameters.prefix"),
    metavar="PREFIX",
    type=str,
    show_envvar=True,
    default="",
)
@click.option(
    "--az-storage-sas-url",
    help=T("csm-data.commands.storage.az_storage_upload.parameters.az_storage_sas_url"),
    type=str,
    show_envvar=True,
    metavar="URL",
    envvar="AZURE_STORAGE_SAS_URL",
)
@web_help("csm-data/az-storage-upload")
@translate_help("csm-data.commands.storage.az_storage_upload.description")
def az_storage_upload(
    source_folder,
    blob_name: str,
    az_storage_sas_url: str,
    file_prefix: str = "",
    recursive: bool = False,
):
    # Import the function at the start of the command
    from cosmotech.coal.azure.storage import upload_folder

    # Upload files to Azure Blob Storage
    upload_folder(
        source_folder=source_folder,
        blob_name=blob_name,
        az_storage_sas_url=az_storage_sas_url,
        file_prefix=file_prefix,
        recursive=recursive,
    )
