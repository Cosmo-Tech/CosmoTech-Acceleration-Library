import json
import os
from pathlib import Path

from cosmotech.coal.utils.configuration import ENVIRONMENT_CONFIGURATION as EC
from cosmotech.coal.utils.logger import LOGGER


class InputCollector:
    def __init__(self):
        self.dataset_collector = DatasetCollector()
        self.parameter_collector = ParameterCollector()
        self.workspace_collector = WorkspaceCollector()

    def fetch_dataset(self, dataset_name: str) -> Path:
        return self.dataset_collector.fetch(dataset_name)

    def fetch_parameter(self, param_name: str) -> Path:
        return self.parameter_collector.fetch(param_name)

    def fetch_workspace_file(self, file_name: str) -> Path:
        return self.workspace_collector.fetch(file_name)

    def fetch(self, name: str) -> Path:
        try:
            return self.fetch_parameter(name)
        except (KeyError, FileNotFoundError):
            LOGGER.debug(f"Parameter {name} not found, trying workspace files.")
        try:
            return self.fetch_workspace_file(name)
        except FileNotFoundError:
            LOGGER.debug(f"Workspace file {name} not found, trying dataset files.")
        return self.fetch_dataset(name)


class DatasetCollector:
    def __init__(self):
        self.paths: dict[str, Path] = {}

    def collect(self):
        for dataset_id in os.listdir(EC.cosmotech.dataset_absolute_path):
            for r, d, f in os.walk(Path(EC.cosmotech.dataset_absolute_path) / dataset_id):
                for dataset_name in f:
                    path = Path(r) / dataset_name
                    self.paths[dataset_name] = path
                    self.paths[path.stem] = path

    def fetch(self, dataset_name: str) -> Path:
        # lazy collection to avoid unnecessary os.walk calls
        if not self.paths:
            self.collect()
        if dataset_name in self.paths:
            return self.paths[dataset_name]
        raise FileNotFoundError(f"File for {dataset_name} not found in {EC.cosmotech.dataset_absolute_path}.")


class WorkspaceCollector:
    def __init__(self):
        self.paths: dict[str, Path] = {}

    def collect(self):
        workspace_path = Path(EC.cosmotech.dataset_absolute_path) / "workspace_files"
        if workspace_path.exists():
            for r, d, f in os.walk(workspace_path):
                for file_name in f:
                    path = Path(r) / file_name
                    self.paths[file_name] = path
                    self.paths[path.stem] = path

    def fetch(self, file_name: str) -> Path:
        if not self.paths:
            self.collect()
        if file_name in self.paths:
            return self.paths[file_name]
        raise FileNotFoundError(f"File {file_name} not found in workspace_files.")


class ParameterCollector:
    def __init__(self):
        self.paths: dict[str, Path] = {}
        self.parameters: dict[str, str] = {}

    def read_parameters_json(self):
        parameter_file = Path(EC.cosmotech.parameters_absolute_path) / "parameters.json"
        if parameter_file.exists():
            with open(parameter_file) as f:
                parameters = json.load(f)
                for parameter in parameters:
                    self.parameters[parameter["parameterId"]] = parameter["value"]

    def collect(self):
        for dataset_id in os.listdir(EC.cosmotech.parameters_absolute_path):
            for r, d, f in os.walk(Path(EC.cosmotech.parameters_absolute_path) / dataset_id):
                for file_name in f:
                    path = Path(r) / file_name
                    param_name = path.parent.name
                    self.paths[param_name] = path
                    self.paths[path.stem] = path

    def fetch_parameter(self, param_name: str) -> Path:
        # lazy collection to avoid unnecessary json loading
        if not self.parameters:
            self.read_parameters_json()
        return self.parameters[param_name]

    def fetch_file_path(self, param_name: str) -> Path:
        # lazy collection to avoid unnecessary os.walk calls
        if not self.paths:
            self.collect()
        if param_name in self.paths:
            return self.paths[param_name]
        raise FileNotFoundError(f"File for {param_name} not found in {EC.cosmotech.parameters_absolute_path}.")

    def fetch(self, param_name: str) -> Path:
        try:
            return self.fetch_parameter(param_name)
        except KeyError:
            return self.fetch_file_path(param_name)


ENVIRONMENT_INPUT_COLLECTOR = InputCollector()
