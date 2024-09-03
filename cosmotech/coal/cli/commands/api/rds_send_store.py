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
from cosmotech.coal.store.store import Store
from cosmotech.coal.store.native_python import convert_table_as_pylist

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@click.option("--store-folder",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              help="The folder containing the store files",
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
@web_help("csm-data/api/rds-send-store")
def rds_send_store(
    store_folder,
    organization_id,
    workspace_id,
    runner_id,
    run_id
):
    """Send all CoAL Datastore content to the results service of the Cosmo Tech API

Requires a valid connection to the API to send the data
    """

    source_dir = pathlib.Path(store_folder)

    if not source_dir.exists():
        LOGGER.error(f"{source_dir} does not exists")
        return 1

    with get_api_client()[0] as api_client:

        api_run = RunApi(api_client)
        _s = Store()
        for table_name in _s.list_tables():
            data = convert_table_as_pylist(table_name)
            fieldnames = _s.get_table_schema(table_name).names
            for row in data:
                for field in fieldnames:
                    if row[field] is None:
                        del row[field]
            LOGGER.info(f"Sending data to table [cyan bold]CD_{table_name}[/]")
            LOGGER.debug(f"  - Column list: {fieldnames}")
            LOGGER.info(f"  - Sending {len(data)} rows")
            api_run.send_run_data(organization_id,
                                  workspace_id,
                                  runner_id,
                                  run_id,
                                  SendRunDataRequest(id=table_name,
                                                     data=data))
