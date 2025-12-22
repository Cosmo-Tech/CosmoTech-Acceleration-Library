# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.utils.configuration import Configuration
from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import require_env, translate_help, web_help


@click.command()
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    show_envvar=True,
    help=T("csm_data.commands.api.run_load_data.parameters.organization_id"),
    metavar="o-##########",
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    show_envvar=True,
    help=T("csm_data.commands.api.run_load_data.parameters.workspace_id"),
    metavar="w-##########",
    required=True,
)
@click.option(
    "--runner-id",
    envvar="CSM_RUNNER_ID",
    show_envvar=True,
    help=T("csm_data.commands.api.run_load_data.parameters.runner_id"),
    metavar="s-##########",
    required=True,
)
@click.option(
    "--dataset-absolute-path",
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    show_envvar=True,
    help=T("csm_data.commands.api.run_load_data.parameters.dataset_absolute_path"),
    metavar="PATH",
    required=True,
)
@click.option(
    "--parameters-absolute-path",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    metavar="PATH",
    show_envvar=True,
    help=T("csm_data.commands.api.run_load_data.parameters.parameters_absolute_path"),
    required=True,
)
@require_env("CSM_API_SCOPE", "The identification scope of a Cosmotech API")
@require_env("CSM_API_URL", "The URL to a Cosmotech API")
@web_help("csm-data/api/run-load-data")
@translate_help("csm_data.commands.api.run_load_data.description")
def run_load_data(
    runner_id: str,
    workspace_id: str,
    organization_id: str,
    dataset_absolute_path: str,
    parameters_absolute_path: str,
):
    # Import the function at the start of the command
    from cosmotech.coal.cosmotech_api.apis.runner import RunnerApi

    _configuration = Configuration()
    _configuration.cosmotech.organization_id = organization_id
    _configuration.cosmotech.workspace_id = workspace_id
    _configuration.cosmotech.runner_id = runner_id
    _configuration.cosmotech.parameters_absolute_path = parameters_absolute_path
    _configuration.cosmotech.dataset_absolute_path = dataset_absolute_path

    _r = RunnerApi(_configuration)

    return _r.download_runner_data(download_datasets=True)


if __name__ == "__main__":
    run_load_data()
