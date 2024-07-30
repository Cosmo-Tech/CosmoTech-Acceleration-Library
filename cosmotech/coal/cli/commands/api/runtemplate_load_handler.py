# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib
from io import BytesIO
from zipfile import BadZipfile
from zipfile import ZipFile

from cosmotech_api.api.solution_api import SolutionApi
from cosmotech_api.api.workspace_api import Workspace
from cosmotech_api.api.workspace_api import WorkspaceApi
from cosmotech_api.exceptions import ServiceException

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              show_envvar=True,
              help="The id of an organization in the cosmotech api",
              metavar="o-##########",
              required=True)
@click.option("--workspace-id",
              envvar="CSM_WORKSPACE_ID",
              show_envvar=True,
              help="The id of a solution in the cosmotech api",
              metavar="w-##########",
              required=True)
@click.option("--run-template-id",
              envvar="CSM_RUN_TEMPLATE_ID",
              show_envvar=True,
              help="The name of the run template in the cosmotech api",
              metavar="NAME",
              required=True)
@click.option("--handler-list",
              envvar="CSM_CONTAINER_MODE",
              show_envvar=True,
              help="A list of handlers to download (comma separated)",
              metavar="HANDLER,...,HANDLER",
              required=True)
@web_help("csm-data/api/runtemplate-load-handler")
def runtemplate_load_handler(workspace_id, organization_id, run_template_id, handler_list):
    """
Uses environment variables to download cloud based Template steps
    """

    has_errors = False
    with get_api_client()[0] as api_client:
        api_w = WorkspaceApi(api_client)

        LOGGER.info("Loading Workspace information to get Solution ID")
        try:
            r_data: Workspace = api_w.find_workspace_by_id(organization_id=organization_id, workspace_id=workspace_id)
        except ServiceException as e:
            LOGGER.error(f"Workspace [green bold]{workspace_id}[/] was not found "
                         f"in Organization [green bold]{organization_id}[/]")
            LOGGER.debug(e.body)
            raise click.Abort()
        solution_id = r_data.solution.solution_id

        api_sol = SolutionApi(api_client)
        handler_list = handler_list.replace("handle-parameters", "parameters_handler")
        root_path = pathlib.Path("../csm_orc_port")
        template_path = root_path / run_template_id
        for handler_id in handler_list.split(','):
            handler_path: pathlib.Path = template_path / handler_id
            LOGGER.info(f"Querying Handler [green bold]{handler_id}[/] for [green bold]{run_template_id}[/]")
            try:
                rt_data = api_sol.download_run_template_handler(organization_id=organization_id,
                                                                solution_id=solution_id,
                                                                run_template_id=run_template_id,
                                                                handler_id=handler_id)
            except ServiceException as e:
                LOGGER.error(
                    f"Handler [green bold]{handler_id}[/] was not found "
                    f"for Run Template [green bold]{run_template_id}[/] "
                    f"in Solution [green bold]{solution_id}[/]")
                LOGGER.debug(e.body)
                has_errors = True
                continue
            LOGGER.info(f"Extracting handler to {handler_path.absolute()}")
            handler_path.mkdir(parents=True, exist_ok=True)

            try:
                with ZipFile(BytesIO(rt_data)) as _zip:
                    _zip.extractall(handler_path)
            except BadZipfile:
                LOGGER.error(f"Handler [green bold]{handler_id}[/] is not a [blue]zip file[/]")
                has_errors = True
        if has_errors:
            LOGGER.error("Issues were met during run, please check the previous logs")
            raise click.Abort()


if __name__ == "__main__":
    runtemplate_load_handler()
