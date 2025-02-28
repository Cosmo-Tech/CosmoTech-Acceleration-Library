# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib

from azure.storage.blob import ContainerClient

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help, translate_help
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


@click.command()
@click.option(
    "--source-folder",
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    help=T("coal-help.commands.storage.az_storage_upload.parameters.source_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--recursive/--no-recursive",
    default=False,
    help=T("coal-help.commands.storage.az_storage_upload.parameters.recursive"),
    type=bool,
    is_flag=True,
)
@click.option(
    "--blob-name",
    envvar="AZURE_STORAGE_BLOB_NAME",
    help=T("coal-help.commands.storage.az_storage_upload.parameters.blob_name"),
    metavar="BUCKET",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--prefix",
    "file_prefix",
    envvar="CSM_DATA_BLOB_PREFIX",
    help=T("coal-help.commands.storage.az_storage_upload.parameters.prefix"),
    metavar="PREFIX",
    type=str,
    show_envvar=True,
    default="",
)
@click.option(
    "--az-storage-sas-url",
    help=T(
        "coal-help.commands.storage.az_storage_upload.parameters.az_storage_sas_url"
    ),
    type=str,
    show_envvar=True,
    metavar="URL",
    envvar="AZURE_STORAGE_SAS_URL",
)
@web_help("csm-data/az-storage-upload")
@translate_help("coal-help.commands.storage.az_storage_upload.description")
def az_storage_upload(
    source_folder,
    blob_name: str,
    az_storage_sas_url: str,
    file_prefix: str = "",
    recursive: bool = False,
):
    source_path = pathlib.Path(source_folder)
    if not source_path.exists():
        LOGGER.error(
            T("coal.errors.file_system.file_not_found").format(
                source_folder=source_folder
            )
        )
        raise FileNotFoundError(
            T("coal.errors.file_system.file_not_found").format(
                source_folder=source_folder
            )
        )

    def file_upload(file_path: pathlib.Path, file_name: str):
        uploaded_file_name = blob_name + "/" + file_prefix + file_name
        LOGGER.info(
            T("coal.logs.data_transfer.file_sent").format(
                file_path=file_path, uploaded_name=uploaded_file_name
            )
        )
        ContainerClient.from_container_url(az_storage_sas_url).upload_blob(
            uploaded_file_name, file_path.open("rb"), overwrite=True
        )

    if source_path.is_dir():
        _source_name = str(source_path)
        for _file_path in source_path.glob("**/*" if recursive else "*"):
            if _file_path.is_file():
                _file_name = (
                    str(_file_path).removeprefix(_source_name).removeprefix("/")
                )
                file_upload(_file_path, _file_name)
    else:
        file_upload(source_path, source_path.name)
