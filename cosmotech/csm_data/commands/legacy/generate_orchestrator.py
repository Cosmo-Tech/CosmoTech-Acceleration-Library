# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import web_help, translate_help
from cosmotech.coal.cosmotech_api import generate_from_solution
from cosmotech.coal.utils.api import get_solution
from cosmotech.coal.utils.api import read_solution_file
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


@click.group()
@web_help("csm-data/legacy/generate-orchestrator")
@translate_help("csm-data.commands.legacy.generate_orchestrator.description")
def generate_orchestrator():
    pass


@generate_orchestrator.command()
@click.argument(
    "solution_file",
    type=click.Path(file_okay=True, dir_okay=False, readable=True, writable=True),
    required=True,
    nargs=1,
)
@click.argument(
    "output",
    type=click.Path(file_okay=True, dir_okay=False, readable=True, writable=True),
    required=True,
    nargs=1,
)
@click.argument("run-template-id", required=True)
@click.option(
    "--describe/--no-describe",
    show_default=True,
    default=False,
    help="Show a description of the generated template after generation",
)
@web_help("csm-data/legacy/generate-orchestrator/from-file")
@translate_help("csm-data.commands.legacy.generate_orchestrator.from_file.description")
def from_file(solution_file, run_template_id, output, describe):
    if _solution := read_solution_file(solution_file):
        return generate_from_solution(
            sol=_solution,
            run_template_id=run_template_id,
            output=output,
            describe=describe,
        )
    return 1


@generate_orchestrator.command()
@click.argument(
    "output",
    type=click.Path(file_okay=True, dir_okay=False, readable=True, writable=True),
    required=True,
    nargs=1,
)
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    show_envvar=True,
    help=T("csm-data.commands.legacy.generate_orchestrator.from_api.parameters.organization_id"),
    metavar="o-##########",
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    show_envvar=True,
    help=T("csm-data.commands.legacy.generate_orchestrator.from_api.parameters.workspace_id"),
    metavar="w-##########",
    required=True,
)
@click.option(
    "--run-template-id",
    envvar="CSM_RUN_TEMPLATE_ID",
    show_envvar=True,
    help=T("csm-data.commands.legacy.generate_orchestrator.from_api.parameters.run_template_id"),
    metavar="NAME",
    required=True,
)
@click.option(
    "--describe/--no-describe",
    show_default=True,
    default=False,
    help=T("csm-data.commands.legacy.generate_orchestrator.from_api.parameters.describe"),
)
@web_help("csm-data/legacy/generate-orchestrator/from-api")
@translate_help("csm-data.commands.legacy.generate_orchestrator.from_api.description")
def from_api(workspace_id, organization_id, run_template_id, output, describe):
    if sol := get_solution(organization_id=organization_id, workspace_id=workspace_id):
        return generate_from_solution(sol=sol, run_template_id=run_template_id, output=output, describe=describe)
    return 1


if __name__ == "__main__":
    generate_orchestrator()
