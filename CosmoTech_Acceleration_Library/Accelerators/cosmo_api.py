# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import io
import os

import cosmotech_api
from azure.identity import DefaultAzureCredential
from cosmotech_api.api.scenario_api import ScenarioApi
from cosmotech_api.api.workspace_api import WorkspaceApi


from CosmoTech_Acceleration_Library.Accelerators.utils.multi_environment import MultiEnvironment

env = MultiEnvironment()


def __get_configuration():
    api_url = env.api_host
    api_scope = env.api_scope
    credentials = DefaultAzureCredential()
    token = credentials.get_token(api_scope)

    configuration = cosmotech_api.Configuration(
        host=api_url,
        discard_unknown_keys=True,
        access_token=token.token
    )
    return configuration


def send_file_to_api(file_content, file_name: str):
    organization_id = os.environ.get("CSM_ORGANIZATION_ID")
    workspace_id = os.environ.get("CSM_WORKSPACE_ID")

    with cosmotech_api.ApiClient(__get_configuration()) as api_client:
        api_ws = WorkspaceApi(api_client)
        api_ws.upload_workspace_file(organization_id=organization_id,
                                     workspace_id=workspace_id,
                                     file=file_content,
                                     overwrite=True,
                                     destination=file_name)


def send_dataframe_to_api(dataframe, file_name: str):
    file_content = io.StringIO()
    dataframe.to_csv(file_content, index=False)
    file_content.seek(0)
    file_content.name = file_name.split('/')[-1]
    send_file_to_api(file_content, file_name)


def get_current_scenario_data():
    """
    Uses environment vars to find the current scenario data from the cosmotech api
    :return: a dict containing the data of the scenario from the API or None in another context
    """
    organization_id = os.environ.get("CSM_ORGANIZATION_ID")
    workspace_id = os.environ.get("CSM_WORKSPACE_ID")
    scenario_id = os.environ.get("CSM_SCENARIO_ID")

    if not all([organization_id, workspace_id, scenario_id]):
        return None

    with cosmotech_api.ApiClient(__get_configuration()) as api_client:
        api_instance = ScenarioApi(api_client)
        scenario_data = api_instance.find_scenario_by_id(organization_id=organization_id,
                                                         workspace_id=workspace_id,
                                                         scenario_id=scenario_id)
    return scenario_data
