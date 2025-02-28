# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import web_help, translate_help
from cosmotech.orchestrator.utils.translate import T


@click.command()
@web_help("csm-data/api/runtemplate-load-handler")
@translate_help("csm-data.commands.api.runtemplate_load_handler.description")
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    show_envvar=True,
    help=T("csm-data.commands.api.runtemplate_load_handler.parameters.organization_id"),
    metavar="o-##########",
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    show_envvar=True,
    help=T("csm-data.commands.api.runtemplate_load_handler.parameters.workspace_id"),
    metavar="w-##########",
    required=True,
)
@click.option(
    "--run-template-id",
    envvar="CSM_RUN_TEMPLATE_ID",
    show_envvar=True,
    help=T("csm-data.commands.api.runtemplate_load_handler.parameters.run_template_id"),
    metavar="NAME",
    required=True,
)
@click.option(
    "--handler-list",
    envvar="CSM_CONTAINER_MODE",
    show_envvar=True,
    help=T("csm-data.commands.api.runtemplate_load_handler.parameters.handler_list"),
    metavar="HANDLER,...,HANDLER",
    required=True,
)
def runtemplate_load_handler(workspace_id, organization_id, run_template_id, handler_list):
    # Import the function at the start of the command
    from cosmotech.coal.cosmotech_api import load_run_template_handlers

    try:
        success = load_run_template_handlers(
            organization_id=organization_id,
            workspace_id=workspace_id,
            run_template_id=run_template_id,
            handler_list=handler_list,
        )
        if not success:
            raise click.Abort()
    except ValueError:
        raise click.Abort()


if __name__ == "__main__":
    runtemplate_load_handler()
