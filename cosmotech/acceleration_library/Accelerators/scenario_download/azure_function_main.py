# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import azure.functions as func
from .scenario_downloader import ScenarioDownloader

import json


def generate_main(apply_update):
    def main(req: func.HttpRequest) -> func.HttpResponse:
        scenario_id = req.params.get('scenario-id')
        organization_id = req.params.get('organization-id')
        workspace_id = req.params.get('workspace-id')

        if scenario_id is None or organization_id is None or workspace_id is None:
            return func.HttpResponse(body='Query is missing configuration', status_code=400)

        dl = ScenarioDownloader(workspace_id=workspace_id,
                                organization_id=organization_id)

        content = dict()

        content['datasets'] = dl.get_all_datasets(scenario_id=scenario_id)
        content['parameters'] = dl.get_all_parameters(scenario_id=scenario_id)

        scenario_data = dl.get_scenario_data(scenario_id=scenario_id)

        updated_content = apply_update(content=content, scenario_data=scenario_data)

        return func.HttpResponse(body=json.dumps(updated_content), headers={"Content-Type": "application/json"})
    return main
