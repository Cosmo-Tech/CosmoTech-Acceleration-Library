# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Orchestration functions for downloading runner and run data.
"""

import os
import pathlib
import shutil
from typing import Dict, List, Any, Optional

from azure.identity import DefaultAzureCredential
from cosmotech_api.api.runner_api import RunnerApi
from cosmotech_api.exceptions import ApiException

from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.runner.data import get_runner_data
from cosmotech.coal.cosmotech_api.runner.parameters import (
    format_parameters_list,
    write_parameters,
)
from cosmotech.coal.cosmotech_api.runner.datasets import (
    get_dataset_ids_from_runner,
    download_datasets,
    dataset_to_file,
)
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def download_runner_data(
    organization_id: str,
    workspace_id: str,
    runner_id: str,
    parameter_folder: str,
    dataset_folder: Optional[str] = None,
    read_files: bool = False,
    parallel: bool = True,
    write_json: bool = True,
    write_csv: bool = False,
    fetch_dataset: bool = True,
) -> Dict[str, Any]:
    """
    Download all runner data including datasets and parameters.

    Args:
        organization_id: Organization ID
        workspace_id: Workspace ID
        runner_id: Runner ID
        parameter_folder: Folder to save parameters
        dataset_folder: Folder to save datasets (if None, only saves datasets referenced by parameters)
        read_files: Whether to read file contents
        parallel: Whether to download datasets in parallel
        write_json: Whether to write parameters as JSON
        write_csv: Whether to write parameters as CSV
        fetch_dataset: Whether to fetch datasets

    Returns:
        Dictionary with runner data, datasets, and parameters
    """
    LOGGER.info(T("coal.cosmotech_api.runner.starting_download"))

    # Get credentials if needed
    credentials = None
    if get_api_client()[1] == "Azure Entra Connection":
        credentials = DefaultAzureCredential()

    # Get runner data
    runner_data = get_runner_data(organization_id, workspace_id, runner_id)

    # Create result dictionary
    result = {"runner_data": runner_data, "datasets": {}, "parameters": {}}

    # Skip if no parameters found
    if not runner_data.parameters_values:
        LOGGER.warning(T("coal.cosmotech_api.runner.no_parameters"))
        return result

    LOGGER.info(T("coal.cosmotech_api.runner.loaded_data"))

    # Format parameters
    parameters = format_parameters_list(runner_data)
    result["parameters"] = {param["parameterId"]: param["value"] for param in parameters}

    # Download datasets if requested
    if fetch_dataset:
        dataset_ids = get_dataset_ids_from_runner(runner_data)

        if dataset_ids:
            LOGGER.info(T("coal.cosmotech_api.runner.downloading_datasets").format(count=len(dataset_ids)))

            datasets = download_datasets(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_ids=dataset_ids,
                read_files=read_files,
                parallel=parallel,
                credentials=credentials,
            )

            result["datasets"] = datasets

            # Process datasets
            datasets_parameters_ids = {
                param.value: param.parameter_id
                for param in runner_data.parameters_values
                if param.var_type == "%DATASETID%" and param.value
            }

            # Save datasets to parameter folders
            for dataset_id, dataset_info in datasets.items():
                # If dataset is referenced by a parameter, save to parameter folder
                if dataset_id in datasets_parameters_ids:
                    param_id = datasets_parameters_ids[dataset_id]
                    param_dir = os.path.join(parameter_folder, param_id)
                    pathlib.Path(param_dir).mkdir(exist_ok=True, parents=True)

                    dataset_folder_path = dataset_to_file(dataset_info)
                    shutil.copytree(dataset_folder_path, param_dir, dirs_exist_ok=True)

                    # Update parameter value to point to the folder
                    for param in parameters:
                        if param["parameterId"] == param_id:
                            param["value"] = param_dir
                            break

                # If dataset is in dataset_list and dataset_folder is provided, save there too
                if dataset_folder and dataset_id in runner_data.dataset_list:
                    pathlib.Path(dataset_folder).mkdir(parents=True, exist_ok=True)
                    dataset_folder_path = dataset_to_file(dataset_info)
                    shutil.copytree(dataset_folder_path, dataset_folder, dirs_exist_ok=True)
                    LOGGER.debug(
                        T("coal.cosmotech_api.runner.dataset_debug").format(folder=dataset_folder, id=dataset_id)
                    )

    # Write parameters to files
    if write_json or write_csv:
        LOGGER.info(T("coal.cosmotech_api.runner.writing_parameters"))
        write_parameters(parameter_folder, parameters, write_csv, write_json)

    return result
