# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Dataset handling functions.
"""

import multiprocessing
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple

from azure.identity import DefaultAzureCredential
from cosmotech_api.api.dataset_api import DatasetApi

from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.dataset import (
    convert_graph_dataset_to_files,
    download_adt_dataset,
    download_twingraph_dataset,
    download_legacy_twingraph_dataset,
    download_file_dataset,
)
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def get_dataset_ids_from_runner(runner_data) -> List[str]:
    """
    Extract dataset IDs from runner data.

    Args:
        runner_data: Runner data object

    Returns:
        List of dataset IDs
    """
    dataset_ids = runner_data.dataset_list[:]

    for parameter in runner_data.parameters_values:
        if parameter.var_type == "%DATASETID%" and parameter.value:
            dataset_id = parameter.value
            dataset_ids.append(dataset_id)

    return dataset_ids


def download_dataset(
    organization_id: str,
    workspace_id: str,
    dataset_id: str,
    read_files: bool = True,
    credentials: Optional[DefaultAzureCredential] = None,
) -> Dict[str, Any]:
    """
    Download a single dataset by ID.

    Args:
        organization_id: Organization ID
        workspace_id: Workspace ID
        dataset_id: Dataset ID
        read_files: Whether to read file contents
        credentials: Azure credentials (if None, uses DefaultAzureCredential if needed)

    Returns:
        Dataset information dictionary
    """

    # Get dataset information
    with get_api_client()[0] as api_client:
        api_instance = DatasetApi(api_client)
        dataset = api_instance.find_dataset_by_id(organization_id=organization_id, dataset_id=dataset_id)

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

        # Download based on dataset type
        if is_adt:
            content, folder_path = download_adt_dataset(
                adt_address=parameters["AZURE_DIGITAL_TWINS_URL"],
                credentials=credentials,
            )
            return {
                "type": "adt",
                "content": content,
                "name": dataset.name,
                "folder_path": str(folder_path),
                "dataset_id": dataset_id,
            }

        elif is_legacy_twin_cache:
            twin_cache_name = parameters["TWIN_CACHE_NAME"]
            content, folder_path = download_legacy_twingraph_dataset(
                organization_id=organization_id, cache_name=twin_cache_name
            )
            return {
                "type": "twincache",
                "content": content,
                "name": dataset.name,
                "folder_path": str(folder_path),
                "dataset_id": dataset_id,
            }

        elif is_storage:
            _file_name = parameters["AZURE_STORAGE_CONTAINER_BLOB_PREFIX"].replace("%WORKSPACE_FILE%/", "")
            content, folder_path = download_file_dataset(
                organization_id=organization_id,
                workspace_id=workspace_id,
                file_name=_file_name,
                read_files=read_files,
            )
            return {
                "type": _file_name.split(".")[-1],
                "content": content,
                "name": dataset.name,
                "folder_path": str(folder_path),
                "dataset_id": dataset_id,
                "file_name": _file_name,
            }

        elif is_in_workspace_file:
            _file_name = dataset.source.location
            content, folder_path = download_file_dataset(
                organization_id=organization_id,
                workspace_id=workspace_id,
                file_name=_file_name,
                read_files=read_files,
            )
            return {
                "type": _file_name.split(".")[-1],
                "content": content,
                "name": dataset.name,
                "folder_path": str(folder_path),
                "dataset_id": dataset_id,
                "file_name": _file_name,
            }

        else:
            content, folder_path = download_twingraph_dataset(organization_id=organization_id, dataset_id=dataset_id)
            return {
                "type": "twincache",
                "content": content,
                "name": dataset.name,
                "folder_path": str(folder_path),
                "dataset_id": dataset_id,
            }


def download_dataset_process(
    _dataset_id, organization_id, workspace_id, read_files, credentials, _return_dict, _error_dict
):
    """
    Process function for downloading a dataset in a separate process.

    This function is designed to be used with multiprocessing to download datasets in parallel.
    It downloads a single dataset and stores the result in a shared dictionary.
    If an error occurs, it stores the error message in a shared error dictionary and re-raises the exception.

    Args:
        _dataset_id: Dataset ID to download
        organization_id: Organization ID
        workspace_id: Workspace ID
        read_files: Whether to read file contents
        credentials: Azure credentials (if None, uses DefaultAzureCredential if needed)
        _return_dict: Shared dictionary to store successful download results
        _error_dict: Shared dictionary to store error messages

    Raises:
        Exception: Any exception that occurs during dataset download is re-raised
    """
    try:
        _c = download_dataset(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_id=_dataset_id,
            read_files=read_files,
            credentials=credentials,
        )
        _return_dict[_dataset_id] = _c
    except Exception as e:
        _error_dict[_dataset_id] = f"{type(e).__name__}: {str(e)}"
        raise e


def download_datasets_parallel(
    organization_id: str,
    workspace_id: str,
    dataset_ids: List[str],
    read_files: bool = True,
    credentials: Optional[DefaultAzureCredential] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Download multiple datasets in parallel.

    Args:
        organization_id: Organization ID
        workspace_id: Workspace ID
        dataset_ids: List of dataset IDs
        read_files: Whether to read file contents
        credentials: Azure credentials (if None, uses DefaultAzureCredential if needed)

    Returns:
        Dictionary mapping dataset IDs to dataset information
    """

    # Use multiprocessing to download datasets in parallel
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    error_dict = manager.dict()
    processes = [
        (
            dataset_id,
            multiprocessing.Process(
                target=download_dataset_process,
                args=(dataset_id, organization_id, workspace_id, read_files, credentials, return_dict, error_dict),
            ),
        )
        for dataset_id in dataset_ids
    ]

    LOGGER.info(T("coal.services.dataset.parallel_download").format(count=len(dataset_ids)))

    [p.start() for _, p in processes]
    [p.join() for _, p in processes]

    for dataset_id, p in processes:
        # We might hit the following bug: https://bugs.python.org/issue43944
        # As a workaround, only treat non-null exit code as a real issue if we also have stored an error
        # message
        if p.exitcode != 0 and dataset_id in error_dict:
            raise ChildProcessError(f"Failed to download dataset '{dataset_id}': {error_dict[dataset_id]}")

    return dict(return_dict)


def download_datasets_sequential(
    organization_id: str,
    workspace_id: str,
    dataset_ids: List[str],
    read_files: bool = True,
    credentials: Optional[DefaultAzureCredential] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Download multiple datasets sequentially.

    Args:
        organization_id: Organization ID
        workspace_id: Workspace ID
        dataset_ids: List of dataset IDs
        read_files: Whether to read file contents
        credentials: Azure credentials (if None, uses DefaultAzureCredential if needed)

    Returns:
        Dictionary mapping dataset IDs to dataset information
    """

    return_dict = {}
    error_dict = {}

    LOGGER.info(T("coal.services.dataset.sequential_download").format(count=len(dataset_ids)))

    for dataset_id in dataset_ids:
        try:
            return_dict[dataset_id] = download_dataset(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_id=dataset_id,
                read_files=read_files,
                credentials=credentials,
            )
        except Exception as e:
            error_dict[dataset_id] = f"{type(e).__name__}: {str(e)}"
            raise ChildProcessError(f"Failed to download dataset '{dataset_id}': {error_dict.get(dataset_id, '')}")

    return return_dict


def download_datasets(
    organization_id: str,
    workspace_id: str,
    dataset_ids: List[str],
    read_files: bool = True,
    parallel: bool = True,
    credentials: Optional[DefaultAzureCredential] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Download multiple datasets, either in parallel or sequentially.

    Args:
        organization_id: Organization ID
        workspace_id: Workspace ID
        dataset_ids: List of dataset IDs
        read_files: Whether to read file contents
        parallel: Whether to download in parallel
        credentials: Azure credentials (if None, uses DefaultAzureCredential if needed)

    Returns:
        Dictionary mapping dataset IDs to dataset information
    """
    if not dataset_ids:
        return {}

    if parallel and len(dataset_ids) > 1:
        return download_datasets_parallel(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            read_files=read_files,
            credentials=credentials,
        )
    else:
        return download_datasets_sequential(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            read_files=read_files,
            credentials=credentials,
        )


def dataset_to_file(dataset_info: Dict[str, Any], target_folder: Optional[Union[str, Path]] = None) -> str:
    """
    Convert dataset to files.

    Args:
        dataset_info: Dataset information dictionary
        target_folder: Optional folder to save files (if None, uses temp dir)

    Returns:
        Path to folder containing files
    """
    dataset_type = dataset_info["type"]
    content = dataset_info["content"]

    if dataset_type in ["adt", "twincache"]:
        # Use conversion function
        if target_folder:
            target_folder = convert_graph_dataset_to_files(content, target_folder)
        else:
            target_folder = convert_graph_dataset_to_files(content)
        return str(target_folder)

    # For file datasets, return the folder path
    if "folder_path" in dataset_info:
        return dataset_info["folder_path"]

    # Fallback to creating a temp directory
    if target_folder:
        return str(target_folder)
    else:
        return tempfile.mkdtemp()
