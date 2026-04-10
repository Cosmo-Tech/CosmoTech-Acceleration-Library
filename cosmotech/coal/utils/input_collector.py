import json
import os
from pathlib import Path

from cosmotech.coal.utils.configuration import ENVIRONMENT_CONFIGURATION as EC


class InputCollector:
    def __init__(self):
        self.dataset_collector = DatasetCollector()
        self.parameter_collector = ParameterCollector()

    def fetch_dataset(self, dataset_name: str) -> Path:
        return self.dataset_collector.fetch(dataset_name)

    def fetch_parameter(self, param_name: str) -> Path:
        return self.parameter_collector.fetch(param_name)

    def fetch(self, name: str) -> Path:
        try:
            return self.fetch_parameter(name)
        except (KeyError, FileNotFoundError):
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

    def fetch(self, dataset_name: str) -> Path:
        # lazy collection to avoid unnecessary os.walk calls
        if not self.paths:
            self.collect()
        if dataset_name in self.paths:
            return self.paths[dataset_name]
        raise FileNotFoundError(f"File for {dataset_name} not found in {EC.cosmotech.dataset_absolute_path}.")


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
        if param_name in self.parameters:
            return self.parameters[param_name]
        else:
            return self.fetch_file_path(param_name)


ENVIRONMENT_INPUT_COLLECTOR = InputCollector()
