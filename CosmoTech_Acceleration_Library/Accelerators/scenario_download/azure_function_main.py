# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import azure.functions as func
from .scenario_downloader import ScenarioDownloader

import json
import http
import traceback


def generate_main(apply_update, parallel=True):
    def main(req: func.HttpRequest) -> func.HttpResponse:
        try:
            scenario_id = req.params.get('scenario-id')
            organization_id = req.params.get('organization-id')
            workspace_id = req.params.get('workspace-id')
            access_token: str = req.headers.get("authorization", None)
            if access_token:
                access_token = access_token.split(" ")[1]

            if scenario_id is None or organization_id is None or workspace_id is None:
                return func.HttpResponse(body=f'Invalid request: organization-id={organization_id}, workspace-id={workspace_id}, scenario-id={scenario_id}',
                                         status_code=http.HTTPStatus.BAD_REQUEST)

            dl = ScenarioDownloader(workspace_id=workspace_id,
                                    organization_id=organization_id,
                                    parallel=parallel,
                                    access_token=access_token)

            content = dict()

            content['datasets'] = dl.get_all_datasets(scenario_id=scenario_id)
            content['parameters'] = dl.get_all_parameters(scenario_id=scenario_id)

            scenario_data = dl.get_scenario_data(scenario_id=scenario_id)

            updated_content = apply_update(content=content, scenario_data=scenario_data)

            return func.HttpResponse(body=json.dumps(updated_content), headers={"Content-Type": "application/json"})
        except Exception as e:
            response = {
                'error': getattr(e, 'message', str(e)),
                'type': type(e).__name__,
                'trace': traceback.format_exc()
            }
            return func.HttpResponse(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                body=json.dumps(response),
                headers={"Content-Type": "application/json"},
            )
    return main
