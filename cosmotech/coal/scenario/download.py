# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Orchestration functions for downloading scenario and run data.

This module is deprecated and will be removed in a future version.
Please use the cosmotech.coal.cosmotech_api.runner.download module instead.
"""

import warnings
from typing import Dict, Any, Optional

from cosmotech.coal.cosmotech_api.runner.download import (
    download_run_data as download_run_data_func,
    download_runner_data as download_runner_data_func,
)


def download_scenario_data(
    organization_id: str,
    workspace_id: str,
    scenario_id: str,
    parameter_folder: str,
    dataset_folder: Optional[str] = None,
    read_files: bool = False,
    parallel: bool = True,
    write_json: bool = False,
    write_csv: bool = True,
    fetch_dataset: bool = True,
) -> Dict[str, Any]:
    """
    Download all scenario data including datasets and parameters.

    This function is deprecated and will be removed in a future version.
    Please use download_run_data from cosmotech.coal.cosmotech_api.runner.download instead.

    Args:
        organization_id: Organization ID
        workspace_id: Workspace ID
        scenario_id: Scenario ID
        parameter_folder: Folder to save parameters
        dataset_folder: Folder to save datasets (if None, only saves datasets referenced by parameters)
        read_files: Whether to read file contents
        parallel: Whether to download datasets in parallel
        write_json: Whether to write parameters as JSON
        write_csv: Whether to write parameters as CSV
        fetch_dataset: Whether to fetch datasets

    Returns:
        Dictionary with scenario data, datasets, and parameters
    """
    warnings.warn(
        "download_scenario_data is deprecated and will be removed in a future version. "
        "Please use download_run_data from cosmotech.coal.cosmotech_api.runner.download instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    result = download_run_data_func(
        organization_id=organization_id,
        workspace_id=workspace_id,
        runner_id=scenario_id,
        parameter_folder=parameter_folder,
        dataset_folder=dataset_folder,
        read_files=read_files,
        parallel=parallel,
        write_json=write_json,
        write_csv=write_csv,
        fetch_dataset=fetch_dataset,
    )

    # Rename runner_data to scenario_data for backward compatibility
    if "runner_data" in result:
        result["scenario_data"] = result.pop("runner_data")

    return result
