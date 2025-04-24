# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import time
from pathlib import Path
from typing import Dict, Any, Optional, Union, Tuple

from cosmotech_api import DatasetApi

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T
from cosmotech.coal.cosmotech_api.connection import get_api_client

# Import specific download functions
# These imports are defined here to avoid circular imports
# The functions are imported directly from their modules
from cosmotech.coal.cosmotech_api.dataset.download.adt import download_adt_dataset
from cosmotech.coal.cosmotech_api.dataset.download.twingraph import (
    download_twingraph_dataset,
    download_legacy_twingraph_dataset,
)
from cosmotech.coal.cosmotech_api.dataset.download.file import download_file_dataset


def download_dataset_by_id(
    organization_id: str,
    workspace_id: str,
    dataset_id: str,
    target_folder: Optional[Union[str, Path]] = None,
) -> Tuple[Dict[str, Any], Path]:
    """
    Download dataset by ID.

    Args:
        organization_id: Organization ID
        workspace_id: Workspace ID
        dataset_id: Dataset ID
        target_folder: Optional folder to save files (if None, uses temp dir)

    Returns:
        Tuple of (dataset info dict, folder path)
    """
    start_time = time.time()
    LOGGER.info(T("coal.services.dataset.download_started").format(dataset_type="Dataset"))
    LOGGER.debug(
        T("coal.services.dataset.dataset_downloading").format(organization_id=organization_id, dataset_id=dataset_id)
    )

    with get_api_client()[0] as api_client:
        api_instance = DatasetApi(api_client)

        # Get dataset info
        info_start = time.time()
        dataset = api_instance.find_dataset_by_id(organization_id=organization_id, dataset_id=dataset_id)
        info_time = time.time() - info_start

        LOGGER.debug(
            T("coal.services.dataset.dataset_info_retrieved").format(dataset_name=dataset.name, dataset_id=dataset_id)
        )
        LOGGER.debug(
            T("coal.common.timing.operation_completed").format(operation="dataset info retrieval", time=info_time)
        )

        # Determine dataset type and download
        if dataset.connector is None:
            parameters = []
        else:
            parameters = dataset.connector.parameters_values

        is_adt = "AZURE_DIGITAL_TWINS_URL" in parameters
        is_storage = "AZURE_STORAGE_CONTAINER_BLOB_PREFIX" in parameters
        is_legacy_twin_cache = "TWIN_CACHE_NAME" in parameters and dataset.twingraph_id is None
        is_in_workspace_file = (
            False if dataset.tags is None else "workspaceFile" in dataset.tags or "dataset_part" in dataset.tags
        )

        download_start = time.time()

        if is_adt:
            LOGGER.debug(T("coal.services.dataset.dataset_type_detected").format(type="ADT"))
            content, folder = download_adt_dataset(
                adt_address=parameters["AZURE_DIGITAL_TWINS_URL"],
                target_folder=target_folder,
            )
            dataset_type = "adt"

        elif is_legacy_twin_cache:
            LOGGER.debug(T("coal.services.dataset.dataset_type_detected").format(type="Legacy TwinGraph"))
            twin_cache_name = parameters["TWIN_CACHE_NAME"]
            content, folder = download_legacy_twingraph_dataset(
                organization_id=organization_id,
                cache_name=twin_cache_name,
                target_folder=target_folder,
            )
            dataset_type = "twincache"

        elif is_storage or is_in_workspace_file:
            if is_storage:
                LOGGER.debug(T("coal.services.dataset.dataset_type_detected").format(type="Storage"))
                _file_name = parameters["AZURE_STORAGE_CONTAINER_BLOB_PREFIX"].replace("%WORKSPACE_FILE%/", "")
            else:
                LOGGER.debug(T("coal.services.dataset.dataset_type_detected").format(type="Workspace File"))
                _file_name = dataset.source.location

            content, folder = download_file_dataset(
                organization_id=organization_id,
                workspace_id=workspace_id,
                file_name=_file_name,
                target_folder=target_folder,
            )
            dataset_type = _file_name.split(".")[-1]

        else:
            LOGGER.debug(T("coal.services.dataset.dataset_type_detected").format(type="TwinGraph"))
            content, folder = download_twingraph_dataset(
                organization_id=organization_id,
                dataset_id=dataset_id,
                target_folder=target_folder,
            )
            dataset_type = "twincache"

        download_time = time.time() - download_start
        LOGGER.debug(
            T("coal.common.timing.operation_completed").format(operation="content download", time=download_time)
        )

    # Prepare result
    dataset_info = {"type": dataset_type, "content": content, "name": dataset.name}

    elapsed_time = time.time() - start_time
    LOGGER.info(
        T("coal.common.timing.operation_completed").format(operation="total dataset download", time=elapsed_time)
    )
    LOGGER.info(T("coal.services.dataset.download_completed").format(dataset_type="Dataset"))

    return dataset_info, folder
