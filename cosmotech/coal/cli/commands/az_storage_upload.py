# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib

from azure.storage.blob import ContainerClient

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@click.option("--source-folder",
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              help="The folder/file to upload to the target blob storage",
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--recursive/--no-recursive",
              default=False,
              help="Recursively send the content of every folder inside the starting folder to the blob storage",
              type=bool,
              is_flag=True)
@click.option("--blob-name",
              envvar="AZURE_STORAGE_BLOB_NAME",
              help="The blob name in the Azure Storage service to upload to",
              metavar="BUCKET",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--prefix",
              "file_prefix",
              envvar="CSM_DATA_BLOB_PREFIX",
              help="A prefix by which all uploaded files should start with in the blob storage",
              metavar="PREFIX",
              type=str,
              show_envvar=True,
              default="")
@click.option("--az-storage-sas-url",
              help="SAS url allowing access to the AZ storage container",
              type=str,
              show_envvar=True,
              metavar="URL",
              envvar="AZURE_STORAGE_SAS_URL")
@web_help("csm-data/az-storage-upload")
def az_storage_upload(
    source_folder,
    blob_name: str,
    az_storage_sas_url: str,
    file_prefix: str = "",
    recursive: bool = False
):
    """Upload a folder to an Azure Storage Blob"""
    source_path = pathlib.Path(source_folder)
    if not source_path.exists():
        LOGGER.error(f"{source_folder} does not exists")
        raise FileNotFoundError(f"{source_folder} does not exists")

    def file_upload(file_path: pathlib.Path, file_name: str):
        uploaded_file_name = blob_name + "/" + file_prefix + file_name
        LOGGER.info(f"Sending {file_path} as {uploaded_file_name}")
        ContainerClient.from_container_url(az_storage_sas_url).upload_blob(uploaded_file_name, 
                                                                           file_path.open("rb"),
                                                                           overwrite=True)

    if source_path.is_dir():
        _source_name = str(source_path)
        for _file_path in source_path.glob("**/*" if recursive else "*"):
            if _file_path.is_file():
                _file_name = str(_file_path).removeprefix(_source_name).removeprefix("/")
                file_upload(_file_path, _file_name)
    else:
        file_upload(source_path, source_path.name)
