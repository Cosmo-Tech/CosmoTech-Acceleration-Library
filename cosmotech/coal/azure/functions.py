# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import azure.functions as func
from cosmotech.coal.cosmotech_api.runner.download import download_runner_data
from cosmotech_api.api.runner_api import RunnerApi

import json
import http
import traceback


def generate_main(apply_update, parallel=True):
    def main(req: func.HttpRequest) -> func.HttpResponse:
        try:
            runner_id = req.params.get("scenario-id")  # Keep parameter name for backward compatibility
            organization_id = req.params.get("organization-id")
            workspace_id = req.params.get("workspace-id")
            access_token: str = req.headers.get("authorization", None)
            if access_token:
                access_token = access_token.split(" ")[1]

            if runner_id is None or organization_id is None or workspace_id is None:
                return func.HttpResponse(
                    body=f"Invalid request: organization-id={organization_id}, workspace-id={workspace_id}, scenario-id={runner_id}",
                    status_code=http.HTTPStatus.BAD_REQUEST,
                )

            # Get runner data
            result = download_runner_data(
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
                parameter_folder=None,  # We don't need to save to files
                read_files=True,
                parallel=parallel,
                write_json=False,
                write_csv=False,
                fetch_dataset=True,
            )

            content = {
                "datasets": result["datasets"],
                "parameters": result["parameters"],
            }

            runner_data = result["runner_data"]

            updated_content = apply_update(
                content=content, scenario_data=runner_data
            )  # Keep parameter name for backward compatibility

            return func.HttpResponse(
                body=json.dumps(updated_content),
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            response = {
                "error": getattr(e, "message", str(e)),
                "type": type(e).__name__,
                "trace": traceback.format_exc(),
            }
            return func.HttpResponse(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                body=json.dumps(response),
                headers={"Content-Type": "application/json"},
            )

    return main
