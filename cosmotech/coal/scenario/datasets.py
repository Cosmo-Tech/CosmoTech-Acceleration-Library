# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Dataset handling functions.

This module is deprecated and will be removed in a future version.
Please use the cosmotech.coal.cosmotech_api.runner.datasets module instead.
"""

import warnings
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

from azure.identity import DefaultAzureCredential

from cosmotech.coal.cosmotech_api.runner.datasets import (
    get_dataset_ids_from_runner,
    download_dataset as download_dataset_func,
    download_datasets as download_datasets_func,
    dataset_to_file as dataset_to_file_func,
)


def get_dataset_ids_from_scenario(scenario_data) -> List[str]:
    """
    Extract dataset IDs from scenario data.

    This function is deprecated and will be removed in a future version.
    Please use get_dataset_ids_from_runner from cosmotech.coal.cosmotech_api.runner.datasets instead.

    Args:
        scenario_data: Scenario data object

    Returns:
        List of dataset IDs
    """
    warnings.warn(
        "get_dataset_ids_from_scenario is deprecated and will be removed in a future version. "
        "Please use get_dataset_ids_from_runner from cosmotech.coal.cosmotech_api.runner.datasets instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_dataset_ids_from_runner(scenario_data)


def download_dataset(
    organization_id: str,
    workspace_id: str,
    dataset_id: str,
    read_files: bool = True,
    credentials: Optional[DefaultAzureCredential] = None,
) -> Dict[str, Any]:
    """
    Download a single dataset by ID.

    This function is deprecated and will be removed in a future version.
    Please use download_dataset from cosmotech.coal.cosmotech_api.runner.datasets instead.

    Args:
        organization_id: Organization ID
        workspace_id: Workspace ID
        dataset_id: Dataset ID
        read_files: Whether to read file contents
        credentials: Azure credentials (if None, uses DefaultAzureCredential if needed)

    Returns:
        Dataset information dictionary
    """
    warnings.warn(
        "download_dataset is deprecated and will be removed in a future version. "
        "Please use download_dataset from cosmotech.coal.cosmotech_api.runner.datasets instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return download_dataset_func(
        organization_id=organization_id,
        workspace_id=workspace_id,
        dataset_id=dataset_id,
        read_files=read_files,
        credentials=credentials,
    )


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

    This function is deprecated and will be removed in a future version.
    Please use download_datasets from cosmotech.coal.cosmotech_api.runner.datasets instead.

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
    warnings.warn(
        "download_datasets is deprecated and will be removed in a future version. "
        "Please use download_datasets from cosmotech.coal.cosmotech_api.runner.datasets instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return download_datasets_func(
        organization_id=organization_id,
        workspace_id=workspace_id,
        dataset_ids=dataset_ids,
        read_files=read_files,
        parallel=parallel,
        credentials=credentials,
    )


def dataset_to_file(dataset_info: Dict[str, Any], target_folder: Optional[Union[str, Path]] = None) -> str:
    """
    Convert dataset to files.

    This function is deprecated and will be removed in a future version.
    Please use dataset_to_file from cosmotech.coal.cosmotech_api.runner.datasets instead.

    Args:
        dataset_info: Dataset information dictionary
        target_folder: Optional folder to save files

    Returns:
        Path to folder containing files
    """
    warnings.warn(
        "dataset_to_file is deprecated and will be removed in a future version. "
        "Please use dataset_to_file from cosmotech.coal.cosmotech_api.runner.datasets instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return dataset_to_file_func(dataset_info, target_folder)
