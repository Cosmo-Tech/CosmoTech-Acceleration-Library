# Copyright (C) - 2023 - 2025 - Cosmo Tech
# Licensed under the MIT license.
import warnings
from typing import Dict, Any, Optional

from azure.identity import DefaultAzureCredential

from cosmotech.coal.scenario.data import get_runner_data
from cosmotech.coal.scenario.parameters import get_runner_parameters
from cosmotech.coal.scenario.datasets import (
    download_dataset,
    download_datasets,
    dataset_to_file as dataset_to_file_func,
    get_dataset_ids_from_runner
)
from cosmotech.coal.cosmotech_api.connection import get_api_client


class ScenarioDownloader:
    """
    Backward compatibility class for ScenarioDownloader.
    
    This class is deprecated and will be removed in a future version.
    Please use the functions in the cosmotech.coal.scenario module instead.
    """
    
    def __init__(
        self,
        workspace_id: str,
        organization_id: str,
        access_token: str = None,
        read_files=True,
        parallel=True
    ):
        warnings.warn(
            "ScenarioDownloader is deprecated and will be removed in a future version. "
            "Please use the functions in the cosmotech.coal.scenario module instead.",
            DeprecationWarning,
            stacklevel=2
        )
        
        # Get credentials if needed
        self.credentials = None
        if get_api_client()[1] == "Azure Entra Connection":
            self.credentials = DefaultAzureCredential()
            
        self.workspace_id = workspace_id
        self.organization_id = organization_id
        self.dataset_file_temp_path = dict()
        self.read_files = read_files
        self.parallel = parallel

    def get_scenario_data(self, scenario_id: str):
        """
        Get scenario data from the API.
        
        Args:
            scenario_id: The ID of the scenario
            
        Returns:
            Scenario data object
        """
        # Use the new function from the scenario.data module
        with get_api_client()[0] as api_client:
            from cosmotech_api.api.runner_api import RunnerApi
            api_instance = RunnerApi(api_client)
            scenario_data = api_instance.get_runner(
                organization_id=self.organization_id,
                workspace_id=self.workspace_id,
                runner_id=scenario_id
            )
        return scenario_data

    def download_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """
        Download a single dataset by ID.
        
        Args:
            dataset_id: Dataset ID
            
        Returns:
            Dataset information dictionary
        """
        # Use the new function from the scenario.datasets module
        dataset_info = download_dataset(
            organization_id=self.organization_id,
            workspace_id=self.workspace_id,
            dataset_id=dataset_id,
            read_files=self.read_files,
            credentials=self.credentials
        )
        
        # Store the folder path for backward compatibility
        if 'folder_path' in dataset_info:
            self.dataset_file_temp_path[dataset_id] = dataset_info['folder_path']
            if 'file_name' in dataset_info:
                self.dataset_file_temp_path[dataset_info['file_name']] = dataset_info['folder_path']
        
        return dataset_info

    def get_all_parameters(self, scenario_id: str) -> dict:
        """
        Extract parameters from scenario data.
        
        Args:
            scenario_id: The ID of the scenario
            
        Returns:
            Dictionary mapping parameter IDs to values
        """
        # Get scenario data and extract parameters
        scenario_data = self.get_scenario_data(scenario_id=scenario_id)
        return get_runner_parameters(scenario_data)

    def get_all_datasets(self, scenario_id: str) -> dict:
        """
        Download all datasets for a scenario.
        
        Args:
            scenario_id: The ID of the scenario
            
        Returns:
            Dictionary mapping dataset IDs to dataset information
        """
        # Get scenario data
        scenario_data = self.get_scenario_data(scenario_id=scenario_id)
        
        # Get dataset IDs
        dataset_ids = get_dataset_ids_from_runner(scenario_data)
        
        # Download datasets
        datasets = download_datasets(
            organization_id=self.organization_id,
            workspace_id=self.workspace_id,
            dataset_ids=dataset_ids,
            read_files=self.read_files,
            parallel=self.parallel,
            credentials=self.credentials
        )
        
        # Store folder paths for backward compatibility
        for dataset_id, dataset_info in datasets.items():
            if 'folder_path' in dataset_info:
                self.dataset_file_temp_path[dataset_id] = dataset_info['folder_path']
                if 'file_name' in dataset_info:
                    self.dataset_file_temp_path[dataset_info['file_name']] = dataset_info['folder_path']
        
        return datasets

    def dataset_to_file(self, dataset_id: str, dataset_info: Dict[str, Any], target_folder: Optional[str] = None) -> str:
        """
        Convert dataset to files.
        
        Args:
            dataset_id: Dataset ID
            dataset_info: Dataset information dictionary
            target_folder: Optional folder to save files
            
        Returns:
            Path to folder containing files
        """
        # Use the new function from the scenario.datasets module
        folder_path = dataset_to_file_func(dataset_info, target_folder)
        
        # Store the folder path for backward compatibility
        self.dataset_file_temp_path[dataset_id] = folder_path
        
        return folder_path
