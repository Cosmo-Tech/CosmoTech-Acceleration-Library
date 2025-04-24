# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import require_env
from cosmotech.csm_data.utils.decorators import web_help, translate_help
from cosmotech.orchestrator.utils.translate import T


@click.command()
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    show_envvar=True,
    help=T("csm-data.commands.api.run_load_data.parameters.organization_id"),
    metavar="o-##########",
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    show_envvar=True,
    help=T("csm-data.commands.api.run_load_data.parameters.workspace_id"),
    metavar="w-##########",
    required=True,
)
@click.option(
    "--runner-id",
    envvar="CSM_RUNNER_ID",
    show_envvar=True,
    help=T("csm-data.commands.api.run_load_data.parameters.runner_id"),
    metavar="s-##########",
    required=True,
)
@click.option(
    "--dataset-absolute-path",
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    show_envvar=True,
    help=T("csm-data.commands.api.run_load_data.parameters.dataset_absolute_path"),
    metavar="PATH",
    required=True,
)
@click.option(
    "--parameters-absolute-path",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    metavar="PATH",
    show_envvar=True,
    help=T("csm-data.commands.api.run_load_data.parameters.parameters_absolute_path"),
    required=True,
)
@click.option(
    "--write-json/--no-write-json",
    envvar="WRITE_JSON",
    show_envvar=True,
    default=True,
    show_default=True,
    help=T("csm-data.commands.api.run_load_data.parameters.write_json"),
)
@click.option(
    "--write-csv/--no-write-csv",
    envvar="WRITE_CSV",
    show_envvar=True,
    default=False,
    show_default=True,
    help=T("csm-data.commands.api.run_load_data.parameters.write_csv"),
)
@click.option(
    "--fetch-dataset/--no-fetch-dataset",
    envvar="FETCH_DATASET",
    show_envvar=True,
    default=True,
    show_default=True,
    help=T("csm-data.commands.api.run_load_data.parameters.fetch_dataset"),
)
@click.option(
    "--parallel/--no-parallel",
    envvar="FETCH_DATASETS_IN_PARALLEL",
    show_envvar=True,
    default=True,
    show_default=True,
    help=T("csm-data.commands.api.run_load_data.parameters.parallel"),
)
@require_env("CSM_API_SCOPE", "The identification scope of a Cosmotech API")
@require_env("CSM_API_URL", "The URL to a Cosmotech API")
@web_help("csm-data/api/run-load-data")
@translate_help("csm-data.commands.api.run_load_data.description")
def run_load_data(
    runner_id: str,
    workspace_id: str,
    organization_id: str,
    dataset_absolute_path: str,
    parameters_absolute_path: str,
    write_json: bool,
    write_csv: bool,
    fetch_dataset: bool,
    parallel: bool,
):
    # Import the function at the start of the command
    from cosmotech.coal.cosmotech_api.runner.download import download_runner_data

    return download_runner_data(
        organization_id=organization_id,
        workspace_id=workspace_id,
        runner_id=runner_id,
        parameter_folder=parameters_absolute_path,
        dataset_folder=dataset_absolute_path,
        read_files=False,
        parallel=parallel,
        write_json=write_json,
        write_csv=write_csv,
        fetch_dataset=fetch_dataset,
    )


if __name__ == "__main__":
    run_load_data()
