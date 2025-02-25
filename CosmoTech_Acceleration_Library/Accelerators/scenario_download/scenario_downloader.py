# Copyright (C) - 2023 - 2025 - Cosmo Tech
# Licensed under the MIT license.
import multiprocessing
import tempfile
from pathlib import Path
from typing import Union, Dict, Any

from azure.identity import DefaultAzureCredential
from cosmotech_api import DatasetApi
from cosmotech_api import ScenarioApi

from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.dataset.utils import get_content_from_twin_graph_data, sheet_to_header
from cosmotech.coal.dataset.converters import convert_graph_dataset_to_files
from cosmotech.coal.dataset.download.adt import download_adt_dataset
from cosmotech.coal.dataset.download.twingraph import (
    download_twingraph_dataset,
    download_legacy_twingraph_dataset
)
from cosmotech.coal.dataset.download.file import download_file_dataset


class ScenarioDownloader:
    def __init__(
        self,
        workspace_id: str,
        organization_id: str,
        access_token: str = None,
        read_files=True,
        parallel=True
    ):
        if get_api_client()[1] == "Azure Entra Connection":
            self.credentials = DefaultAzureCredential()
        else:
            self.credentials = None

        self.workspace_id = workspace_id
        self.organization_id = organization_id
        self.dataset_file_temp_path = dict()
        self.read_files = read_files
        self.parallel = parallel

    def get_scenario_data(self, scenario_id: str):
        with get_api_client()[0] as api_client:
            api_instance = ScenarioApi(api_client)
            scenario_data = api_instance.find_scenario_by_id(organization_id=self.organization_id,
                                                             workspace_id=self.workspace_id,
                                                             scenario_id=scenario_id)
        return scenario_data

    def download_dataset(self, dataset_id: str) -> Dict[str, Any]:
        with get_api_client()[0] as api_client:
            api_instance = DatasetApi(api_client)
            dataset = api_instance.find_dataset_by_id(
                organization_id=self.organization_id,
                dataset_id=dataset_id)
            
            if dataset.connector is None:
                parameters = []
            else:
                parameters = dataset.connector.parameters_values
                
            is_adt = 'AZURE_DIGITAL_TWINS_URL' in parameters
            is_storage = 'AZURE_STORAGE_CONTAINER_BLOB_PREFIX' in parameters
            is_legacy_twin_cache = 'TWIN_CACHE_NAME' in parameters and dataset.twingraph_id is None
            is_in_workspace_file = False if dataset.tags is None else 'workspaceFile' in dataset.tags or 'dataset_part' in dataset.tags

            if is_adt:
                # Use new ADT download function
                content, folder_path = download_adt_dataset(
                    adt_address=parameters['AZURE_DIGITAL_TWINS_URL'],
                    credentials=self.credentials
                )
                return {
                    "type": 'adt',
                    "content": content,
                    "name": dataset.name
                }
                
            elif is_legacy_twin_cache:
                # Use new legacy TwinGraph download function
                twin_cache_name = parameters['TWIN_CACHE_NAME']
                content, folder_path = download_legacy_twingraph_dataset(
                    organization_id=self.organization_id,
                    cache_name=twin_cache_name
                )
                return {
                    "type": "twincache",
                    "content": content,
                    "name": dataset.name
                }
                
            elif is_storage:
                # Use new file download function
                _file_name = parameters['AZURE_STORAGE_CONTAINER_BLOB_PREFIX'].replace(
                    '%WORKSPACE_FILE%/', '')
                content, folder_path = download_file_dataset(
                    organization_id=self.organization_id,
                    workspace_id=self.workspace_id,
                    file_name=_file_name,
                    read_files=self.read_files
                )
                self.dataset_file_temp_path[dataset_id] = str(folder_path)
                self.dataset_file_temp_path[_file_name] = str(folder_path)
                return {
                    "type": _file_name.split('.')[-1],
                    "content": content,
                    "name": dataset.name
                }
                
            elif is_in_workspace_file:
                # Use new file download function
                _file_name = dataset.source.location
                content, folder_path = download_file_dataset(
                    organization_id=self.organization_id,
                    workspace_id=self.workspace_id,
                    file_name=_file_name,
                    read_files=self.read_files
                )
                self.dataset_file_temp_path[dataset_id] = str(folder_path)
                self.dataset_file_temp_path[_file_name] = str(folder_path)
                return {
                    "type": _file_name.split('.')[-1],
                    "content": content,
                    "name": dataset.name
                }
                
            else:
                # Use new TwinGraph download function
                content, folder_path = download_twingraph_dataset(
                    organization_id=self.organization_id,
                    dataset_id=dataset_id
                )
                return {
                    "type": "twincache",
                    "content": content,
                    "name": dataset.name
                }

    def get_all_parameters(self, scenario_id) -> dict:
        scenario_data = self.get_scenario_data(scenario_id=scenario_id)
        content = dict()
        for parameter in scenario_data.parameters_values:
            content[parameter.parameter_id] = parameter.value
        return content

    def get_all_datasets(self, scenario_id: str) -> dict:
        scenario_data = self.get_scenario_data(scenario_id=scenario_id)

        datasets = scenario_data.dataset_list

        dataset_ids = datasets[:]

        for parameter in scenario_data.parameters_values:
            if parameter.var_type == '%DATASETID%' and parameter.value:
                dataset_id = parameter.value
                dataset_ids.append(dataset_id)

        def download_dataset_process(_dataset_id, _return_dict, _error_dict):
            try:
                _c = self.download_dataset(_dataset_id)
                if _dataset_id in self.dataset_file_temp_path:
                    _return_dict[_dataset_id] = (_c, self.dataset_file_temp_path[_dataset_id], _dataset_id)
                else:
                    _return_dict[_dataset_id] = _c
            except Exception as e:
                _error_dict[_dataset_id] = f'{type(e).__name__}: {str(e)}'
                raise e

        if self.parallel and len(dataset_ids) > 1:
            manager = multiprocessing.Manager()
            return_dict = manager.dict()
            error_dict = manager.dict()
            processes = [
                (dataset_id, multiprocessing.Process(target=download_dataset_process,
                                                     args=(dataset_id, return_dict, error_dict)))
                for dataset_id in dataset_ids
            ]
            [p.start() for _, p in processes]
            [p.join() for _, p in processes]

            for dataset_id, p in processes:
                # We might hit the following bug: https://bugs.python.org/issue43944
                # As a workaround, only treat non-null exit code as a real issue if we also have stored an error
                # message
                if p.exitcode != 0 and dataset_id in error_dict:
                    raise ChildProcessError(
                        f"Failed to download dataset '{dataset_id}': {error_dict[dataset_id]}")
        else:
            return_dict = {}
            error_dict = {}
            for dataset_id in dataset_ids:
                try:
                    download_dataset_process(dataset_id, return_dict, error_dict)
                except Exception as e:
                    raise ChildProcessError(
                        f"Failed to download dataset '{dataset_id}': {error_dict.get(dataset_id, '')}")
        content = dict()
        for k, v in return_dict.items():
            if isinstance(v, tuple):
                content[k] = v[0]
                self.dataset_file_temp_path[v[2]] = v[1]
            else:
                content[k] = v
        return content

    def dataset_to_file(self, dataset_id, dataset_info):
        type = dataset_info['type']
        content = dataset_info['content']
        name = dataset_info['name']
        
        if type in ["adt", "twincache"]:
            # Use new conversion function
            target_folder = convert_graph_dataset_to_files(content, None)
            return str(target_folder)
            
        return self.dataset_file_temp_path[dataset_id]
