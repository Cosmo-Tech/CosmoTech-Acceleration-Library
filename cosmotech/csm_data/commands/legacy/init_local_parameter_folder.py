# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import web_help, translate_help
from cosmotech.coal.cosmotech_api import generate_parameters
from cosmotech.coal.utils.api import get_solution
from cosmotech.coal.utils.api import read_solution_file
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


@click.group()
@web_help("csm-data/legacy/init-local-parameter-folder")
@translate_help("csm-data.commands.legacy.init_local_parameter_folder.description")
def init_local_parameter_folder():
    pass


@init_local_parameter_folder.command()
@click.argument(
    "solution_file",
    type=click.Path(file_okay=True, dir_okay=False, readable=True, writable=True),
    required=True,
    nargs=1,
)
@click.argument(
    "output_folder",
    type=click.Path(dir_okay=True, readable=True, writable=True),
    required=True,
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    nargs=1,
)
@click.argument("run_template_id", required=True)
@click.option(
    "--write-json/--no-write-json",
    envvar="WRITE_JSON",
    show_envvar=True,
    default=False,
    show_default=True,
    help="Toggle writing of parameters in json format",
)
@click.option(
    "--write-csv/--no-write-csv",
    envvar="WRITE_CSV",
    show_envvar=True,
    default=True,
    show_default=True,
    help="Toggle writing of parameters in csv format",
)
@web_help("csm-data/legacy/init-local-parameter-folder/solution")
@translate_help("csm-data.commands.legacy.init_local_parameter_folder.solution.description")
def solution(
    solution_file: str,
    run_template_id: str,
    output_folder: str,
    write_json: bool,
    write_csv: bool,
):
    if sol := read_solution_file(solution_file):
        return generate_parameters(sol, run_template_id, output_folder, write_json, write_csv)
    return 1


@init_local_parameter_folder.command()
@click.argument(
    "output_folder",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    type=click.Path(dir_okay=True, readable=True, writable=True),
    required=True,
    nargs=1,
)
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    show_envvar=True,
    help=T("csm-data.commands.legacy.init_local_parameter_folder.cloud.parameters.organization_id"),
    metavar="o-##########",
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    show_envvar=True,
    help=T("csm-data.commands.legacy.init_local_parameter_folder.cloud.parameters.workspace_id"),
    metavar="w-##########",
    required=True,
)
@click.option(
    "--run-template-id",
    envvar="CSM_RUN_TEMPLATE_ID",
    show_envvar=True,
    help=T("csm-data.commands.legacy.init_local_parameter_folder.cloud.parameters.run_template_id"),
    metavar="NAME",
    required=True,
)
@click.option(
    "--write-json/--no-write-json",
    envvar="WRITE_JSON",
    show_envvar=True,
    default=False,
    show_default=True,
    help=T("csm-data.commands.legacy.init_local_parameter_folder.cloud.parameters.write_json"),
)
@click.option(
    "--write-csv/--no-write-csv",
    envvar="WRITE_CSV",
    show_envvar=True,
    default=True,
    show_default=True,
    help=T("csm-data.commands.legacy.init_local_parameter_folder.cloud.parameters.write_csv"),
)
@web_help("csm-data/legacy/init-local-parameter-folder/cloud")
@translate_help("csm-data.commands.legacy.init_local_parameter_folder.cloud.description")
def cloud(
    workspace_id: str,
    organization_id: str,
    run_template_id: str,
    output_folder: str,
    write_json: bool,
    write_csv: bool,
):
    if sol := get_solution(organization_id=organization_id, workspace_id=workspace_id):
        return generate_parameters(sol, run_template_id, output_folder, write_json, write_csv)
    return 1


if __name__ == "__main__":
    init_local_parameter_folder()
