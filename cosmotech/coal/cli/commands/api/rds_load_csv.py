# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib
from csv import DictWriter

from cosmotech_api import RunDataQuery
from cosmotech_api.api.run_api import RunApi

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@click.option("--target-folder",
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              help="The folder where the csv will be written",
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              help="An organization id for the Cosmo Tech API",
              metavar="o-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--workspace-id",
              envvar="CSM_WORKSPACE_ID",
              help="A workspace id for the Cosmo Tech API",
              metavar="w-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--runner-id",
              envvar="CSM_RUNNER_ID",
              help="A runner id for the Cosmo Tech API",
              metavar="r-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--run-id",
              envvar="CSM_RUN_ID",
              help="A run id for the Cosmo Tech API",
              metavar="run-XXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--file-name",
              help="A file name to write the query results",
              metavar="NAME",
              type=str,
              default="results",
              show_default=True,
              required=True)
@click.option("--query",
              help="A run id for the Cosmo Tech API",
              metavar="SQL_QUERY",
              type=str,
              default="SELECT table_name FROM information_schema.tables WHERE table_schema='public'",
              show_default=True)
@web_help("csm-data/api/rds-load-csv")
def rds_load_csv(
    target_folder,
    organization_id,
    workspace_id,
    runner_id,
    run_id,
    file_name,
    query
):
    """Download a CSV file from the Cosmo Tech Run API using a given SQL query

Requires a valid connection to the API to send the data
    """

    target_dir = pathlib.Path(target_folder)

    target_dir.mkdir(parents=True, exist_ok=True)

    with get_api_client()[0] as api_client:
        api_run = RunApi(api_client)
        query = api_run.query_run_data(organization_id,
                                       workspace_id,
                                       runner_id,
                                       run_id,
                                       RunDataQuery(query=query))
        if query.result:
            LOGGER.info(f"Query returned {len(query.result)} rows")
            with open(target_dir / (file_name + ".csv"), "w") as _f:
                headers = set()
                for r in query.result:
                    headers = headers | set(r.keys())
                dw = DictWriter(_f, fieldnames=sorted(headers))
                dw.writeheader()
                dw.writerows(query.result)
            LOGGER.info(f"Results saved as {target_dir / file_name}.csv")
        else:
            LOGGER.info("No results returned by the query")
