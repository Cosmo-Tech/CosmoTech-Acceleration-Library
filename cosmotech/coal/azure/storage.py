# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Azure Storage operations module.

This module provides functions for interacting with Azure Storage, including
uploading files to blob storage.
"""

import pathlib

from azure.storage.blob import ContainerClient

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def upload_file(
    file_path: pathlib.Path,
    blob_name: str,
    az_storage_sas_url: str,
    file_prefix: str = "",
) -> None:
    """
    Upload a single file to Azure Blob Storage.

    Args:
        file_path: Path to the file to upload
        blob_name: Name of the blob container
        az_storage_sas_url: SAS URL for the Azure Storage account
        file_prefix: Prefix to add to the file name in the blob
    """
    uploaded_file_name = blob_name + "/" + file_prefix + file_path.name
    LOGGER.info(T("coal.common.data_transfer.file_sent").format(file_path=file_path, uploaded_name=uploaded_file_name))
    ContainerClient.from_container_url(az_storage_sas_url).upload_blob(
        uploaded_file_name, file_path.open("rb"), overwrite=True
    )


def upload_folder(
    source_folder: str,
    blob_name: str,
    az_storage_sas_url: str,
    file_prefix: str = "",
    recursive: bool = False,
) -> None:
    """
    Upload files from a folder to Azure Blob Storage.

    Args:
        source_folder: Path to the folder containing files to upload
        blob_name: Name of the blob container
        az_storage_sas_url: SAS URL for the Azure Storage account
        file_prefix: Prefix to add to the file names in the blob
        recursive: Whether to recursively upload files from subdirectories
    """
    source_path = pathlib.Path(source_folder)
    if not source_path.exists():
        LOGGER.error(T("coal.common.file_operations.not_found").format(source_folder=source_folder))
        raise FileNotFoundError(T("coal.common.file_operations.not_found").format(source_folder=source_folder))

    if source_path.is_dir():
        _source_name = str(source_path)
        for _file_path in source_path.glob("**/*" if recursive else "*"):
            if _file_path.is_file():
                _file_name = str(_file_path).removeprefix(_source_name).removeprefix("/")
                upload_file(_file_path, blob_name, az_storage_sas_url, file_prefix)
    else:
        upload_file(source_path, blob_name, az_storage_sas_url, file_prefix)
