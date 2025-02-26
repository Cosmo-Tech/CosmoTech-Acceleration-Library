# Copyright (C) - 2023 - 2025 - Cosmo Tech
# Licensed under the MIT license.
import azure.functions as func
from cosmotech.coal.scenario.download import download_scenario_data
from cosmotech_api.api.runner_api import RunnerApi

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

            # Get scenario data using the new functions
            result = download_scenario_data(
                organization_id=organization_id,
                workspace_id=workspace_id,
                scenario_id=scenario_id,
                parameter_folder=None,  # We don't need to save to files
                read_files=True,
                parallel=parallel,
                write_json=False,
                write_csv=False,
                fetch_dataset=True
            )
            
            content = {
                'datasets': result['datasets'],
                'parameters': result['parameters']
            }
            
            scenario_data = result['scenario_data']

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
