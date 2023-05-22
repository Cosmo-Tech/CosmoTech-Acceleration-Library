# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import json
import logging
import os
import pathlib
from distutils.dir_util import copy_tree

from CosmoTech_Acceleration_Library.Accelerators.scenario_download.scenario_downloader import ScenarioDownloader

logging.basicConfig()
logger = logging.getLogger(__name__)


def download_scenario_data(
    organization_id: str, workspace_id: str, scenario_id: str, dataset_folder: str, parameter_folder: str
) -> None:
    """
    Download the datas from a scenario from the CosmoTech API to the local file system
    :param scenario_id: The id of the Scenario as defined in the CosmoTech API
    :param organization_id: The id of the Organization as defined in the CosmoTech API
    :param workspace_id: The id of the Workspace as defined in the CosmoTech API
    :param dataset_folder: a local folder where the main dataset of the scenario will be downloaded
    :param parameter_folder: a local folder where all parameters will be downloaded
    :return: Nothing
    """
    logger.info("Starting connector")
    dl = ScenarioDownloader(workspace_id=workspace_id,
                            organization_id=organization_id,
                            read_files=False)
    logger.info("Initialized downloader")
    content = dict()
    content['datasets'] = dl.get_all_datasets(scenario_id=scenario_id)

    content['parameters'] = dl.get_all_parameters(scenario_id=scenario_id)
    logger.info("Downloaded content")
    dataset_paths = dict()

    dataset_dir = dataset_folder

    for k in content['datasets'].keys():
        dataset_paths[k] = dl.dataset_to_file(k, content['datasets'][k])
        if k not in content['parameters'].values():
            copy_tree(dataset_paths[k], dataset_dir)

    logger.info("Stored datasets")
    tmp_parameter_dir = parameter_folder

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
    logger.info("Generated parameters.json")


def main():
    """
    Uses environment variables to call the download_scenario_data function
    """
    logger.setLevel(logging.INFO)
    import os

    required_environment = [
        ('CSM_API_URL', "The url to a cosmotech api"),
        ('CSM_API_SCOPE', "The identification scope of a cosmotech api"),
        ('CSM_SCENARIO_ID', "The id of a scenario in the cosmotech api"),
        ('CSM_WORKSPACE_ID', "The id of a workspace in the cosmotech api"),
        ('CSM_ORGANIZATION_ID', "The id of an organization in the cosmotech api"),
        ('CSM_DATASET_ABSOLUTE_PATH', "A local folder to store the main dataset content"),
        ('CSM_PARAMETERS_ABSOLUTE_PATH', "A local folder to store the parameters content")
    ]

    s_id = os.environ.get('CSM_SCENARIO_ID')
    w_id = os.environ.get('CSM_WORKSPACE_ID')
    o_id = os.environ.get('CSM_ORGANIZATION_ID')
    dataset_path = os.environ.get('CSM_DATASET_ABSOLUTE_PATH', "/tmp/dataset")
    parameter_path = os.environ.get('CSM_PARAMETERS_ABSOLUTE_PATH', "/tmp/parameters")
    if any(_var not in os.environ for _var, _ in required_environment):
        print("An error exists in the environment to run this command.")
        print("The following values are required")
        for _var, _help in required_environment:
            print(f"  {_var} : {_help}")
        exit(1)
    download_scenario_data(o_id, w_id, s_id, dataset_path, parameter_path)


if __name__ == "__main__":
    main()
