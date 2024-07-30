# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import pathlib
from csv import DictReader

from cosmotech_api import SendRunDataRequest
from cosmotech_api.api.run_api import RunApi

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@click.option("--source-folder",
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              help="The folder containing csvs to send",
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
@web_help("csm-data/api/rds-send-csv")
def rds_send_csv(
    source_folder,
    organization_id,
    workspace_id,
    runner_id,
    run_id
):
    """Send all csv files from a folder to the results service of the Cosmo Tech API

Requires a valid connection to the API to send the data
    """

    source_dir = pathlib.Path(source_folder)

    if not source_dir.exists():
        LOGGER.error(f"{source_dir} does not exists")
        return 1

    with get_api_client()[0] as api_client:

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
