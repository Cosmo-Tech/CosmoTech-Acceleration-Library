# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import scenario_downloader
from distutils.dir_util import copy_tree
import pathlib
import shutil

import json


def dummy_connector(scenario_id, organization_id, workspace_id, dataset_folder, parameter_folder):
    dl = scenario_downloader.ScenarioDownloader(workspace_id=workspace_id,
                                                organization_id=organization_id,
                                                read_files=False)

    content = dict()
    content['datasets'] = dl.get_all_datasets(scenario_id=scenario_id)

    content['parameters'] = dl.get_all_parameters(scenario_id=scenario_id)

    dataset_paths = dict()

    dataset_dir = dataset_folder
    if pathlib.Path(dataset_dir).exists():
        shutil.rmtree(dataset_dir)
    pathlib.Path(dataset_dir).mkdir()

    for k in content['datasets'].keys():
        dataset_paths[k] = dl.dataset_to_file(k, content['datasets'][k])
        if k not in content['parameters'].values():
            copy_tree(dataset_paths[k], dataset_dir)

    tmp_parameter_dir = parameter_folder
    if pathlib.Path(tmp_parameter_dir).exists():
        shutil.rmtree(tmp_parameter_dir)
    pathlib.Path(tmp_parameter_dir).mkdir()
    tmp_parameter_file = os.path.join(tmp_parameter_dir, "parameters.json")

    parameters = []

    for parameter_name, value in content['parameters'].items():
        def add_file_parameter(compared_parameter_name: str, dataset_id: str):
            if parameter_name == compared_parameter_name:
                param_dir = os.path.join(tmp_parameter_dir, compared_parameter_name)
                pathlib.Path(param_dir).mkdir(exist_ok=True)
                dataset_content_path = dataset_paths[dataset_id]
                copy_tree(dataset_content_path, param_dir)
                parameters.append({
                    "parameterId": parameter_name,
                    "value": parameter_name,
                    "varType": "%DATASETID%"
                })

        if value in content['datasets']:
            add_file_parameter(parameter_name, value)
        parameters.append({
            "parameterId": parameter_name,
            "value": value,
            "varType": str(type(value).__name__)
        })

    with open(tmp_parameter_file, "w") as _file:
        json.dump(parameters, _file)


if __name__ == "__main__":
    import os

    s_id = os.environ.get('CSM_SCENARIO_ID')
    w_id = os.environ.get('CSM_WORKSPACE_ID')
    o_id = os.environ.get('CSM_ORGANIZATION_ID')
    dataset_path = os.environ.get('CSM_DATASET_ABSOLUTE_PATH', "/tmp/dataset")
    parameter_path = os.environ.get('CSM_PARAMETERS_ABSOLUTE_PATH', "/tmp/parameters")
    if not (s_id and w_id and o_id):
        print("ERROR WITH ENV")
        exit(1)
    dummy_connector(s_id, o_id, w_id, dataset_path, parameter_path)