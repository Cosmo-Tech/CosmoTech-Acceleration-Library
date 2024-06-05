# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib
from csv import DictWriter

import cosmotech_api
from cosmotech_api import RunDataQuery
from cosmotech_api.api.run_api import RunApi

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@click.option("--target-folder",
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              help="The folder where the csv will be written",
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--api-url",
              envvar="CSM_API_URL",
              help="An URL to a Cosmo Tech API tenant",
              metavar="URL",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--api-key",
              envvar="CSM_API_KEY",
              help="An API key configured in your Cosmo Tech tenant allowed to access runs/data/send",
              metavar="API_KEY",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--api-key-header",
              envvar="CSM_API_KEY_HEADER",
              help="An API key configured in your Cosmo Tech tenant allowed to access runs/data/send",
              metavar="HEADER",
              type=str,
              show_envvar=True,
              show_default=True,
              default="X-CSM-API-KEY")
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              help="An organization id for the Cosmo Tech API",
              metavar="o-XXXXXXXX",
              type=str,
              show_envvar=True)
@click.option("--workspace-id",
              envvar="CSM_WORKSPACE_ID",
              help="A workspace id for the Cosmo Tech API",
              metavar="w-XXXXXXXX",
              type=str,
              show_envvar=True)
@click.option("--runner-id",
              envvar="CSM_RUNNER_ID",
              help="A runner id for the Cosmo Tech API",
              metavar="r-XXXXXXXX",
              type=str,
              show_envvar=True)
@click.option("--run-id",
              envvar="CSM_RUN_ID",
              help="A run id for the Cosmo Tech API",
              metavar="run-XXXXXX",
              type=str,
              show_envvar=True)
@click.option("--file-name",
              help="A file name to write the query results",
              metavar="NAME",
              type=str,
              default="results",
              show_default=True)
@click.option("--query",
              help="A run id for the Cosmo Tech API",
              metavar="run-XXXXXX",
              type=str,
              default="SELECT table_name FROM information_schema.tables WHERE table_schema='public'",
              show_default=True)
def rds_load_csv_command(
    target_folder,
    api_url,
    api_key,
    api_key_header,
    organization_id,
    workspace_id,
    runner_id,
    run_id,
    file_name,
    query
):
    """Send all csv files from a folder to the results service of the Cosmo Tech API

Requires a valid connection to the API to send the data

This implementation make use of an API Key
    """

    target_dir = pathlib.Path(target_folder)

    target_dir.mkdir(parents=True, exist_ok=True)

    configuration = cosmotech_api.Configuration(
        host=api_url,
    )
    with cosmotech_api.ApiClient(configuration,
                                 api_key_header,
                                 api_key) as api_client:
        api_run = RunApi(api_client)
        query = api_run.query_run_data(organization_id,
                                       workspace_id,
                                       runner_id,
                                       run_id,
                                       RunDataQuery(query=query))
        if query.result:
            LOGGER.info(f"Query returned {len(query.result)} rows")
            with open(target_dir / (file_name + ".csv"), "w") as _f:
                headers = query.result[0].keys()
                dw = DictWriter(_f, fieldnames=headers)
                dw.writeheader()
                dw.writerows(query.result)
            LOGGER.info(f"Results saved as {target_dir / file_name}.csv")
        else:
            LOGGER.info("No results returned by the query")
