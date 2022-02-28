# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import os

import cosmotech_api
from cosmotech_api.api.scenario_api import ScenarioApi
from azure.identity import DefaultAzureCredential


def get_current_scenario_data():
    """
    Uses environment vars to find the current scenario data from the cosmotech api
    :return: a dict containing the data of the scenario from the API or None in another context
    """
    api_url = os.environ.get("CSM_API_URL")
    api_scope = os.environ.get("CSM_API_SCOPE")
    organization_id = os.environ.get("CSM_ORGANIZATION_ID")
    workspace_id = os.environ.get("CSM_WORKSPACE_ID")
    scenario_id = os.environ.get("CSM_SCENARIO_ID")

    if not all([api_url, api_scope, organization_id, workspace_id, scenario_id]):
        return None

    credentials = DefaultAzureCredential()
    token = credentials.get_token(api_scope)

    configuration = cosmotech_api.Configuration(
        host=api_url,
        discard_unknown_keys=True,
        access_token=token.token
    )

    with cosmotech_api.ApiClient(configuration) as api_client:
        api_instance = ScenarioApi(api_client)
        scenario_data = api_instance.find_scenario_by_id(organization_id=organization_id,
                                                         workspace_id=workspace_id,
                                                         scenario_id=scenario_id)
    return scenario_data

