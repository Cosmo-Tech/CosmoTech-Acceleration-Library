# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import pathlib
from csv import DictReader

import cosmotech_api
from cosmotech_api import SendRunDataRequest
from cosmotech_api.api.run_api import RunApi

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@click.option("--source-folder",
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              help="The folder containing csvs to send",
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
              help="The header configured in your api to send an API key",
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
def rds_send_csv_command(
    source_folder,
    api_url,
    api_key,
    api_key_header,
    organization_id,
    workspace_id,
    runner_id,
    run_id
):
    """Send all csv files from a folder to the results service of the Cosmo Tech API

Requires a valid connection to the API to send the data

This implementation make use of an API Key
    """

    source_dir = pathlib.Path(source_folder)

    if not source_dir.exists():
        LOGGER.error(f"{source_dir} does not exists")
        return 1

    configuration = cosmotech_api.Configuration(
        host=api_url,
    )
    with cosmotech_api.ApiClient(configuration,
                                 api_key_header,
                                 api_key) as api_client:

        api_run = RunApi(api_client)
        for csv_path in source_dir.glob("*.csv"):
            with open(csv_path) as _f:
                dr = DictReader(_f)
                table_name = csv_path.name.replace(".csv", "")
                LOGGER.info(f"Sending data to table [cyan bold]CD_{table_name}[/]")
                LOGGER.debug(f"  - Column list: {dr.fieldnames}")
                data = []

                for row in dr:
                    n_row = dict()
                    for k, v in row.items():
                        if isinstance(v, str):
                            try:
                                n_row[k] = json.loads(v)
                            except json.decoder.JSONDecodeError:
                                n_row[k] = v
                        else:
                            n_row[k] = v
                    data.append(n_row)

                LOGGER.info(f"  - Sending {len(data)} rows")
                api_run.send_run_data(organization_id,
                                      workspace_id,
                                      runner_id,
                                      run_id,
                                      SendRunDataRequest(id=table_name,
                                                         data=data))
