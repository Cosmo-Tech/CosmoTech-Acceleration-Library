# Copyright (C) - 2023 - 2025 - Cosmo Tech
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
from cosmotech.coal.cli.utils.decorators import web_help, translate_help
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


@click.command()
@click.option("--store-folder",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              help=T("coal-help.commands.api.rds_send_store.parameters.store_folder"),
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              help=T("coal-help.commands.api.rds_send_store.parameters.organization_id"),
              metavar="o-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--workspace-id",
              envvar="CSM_WORKSPACE_ID",
              help=T("coal-help.commands.api.rds_send_store.parameters.workspace_id"),
              metavar="w-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--runner-id",
              envvar="CSM_RUNNER_ID",
              help=T("coal-help.commands.api.rds_send_store.parameters.runner_id"),
              metavar="r-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--run-id",
              envvar="CSM_RUN_ID",
              help=T("coal-help.commands.api.rds_send_store.parameters.run_id"),
              metavar="run-XXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@web_help("csm-data/api/rds-send-store")
@translate_help("coal-help.commands.api.rds_send_store.description")
def rds_send_store(
    store_folder,
    organization_id,
    workspace_id,
    runner_id,
    run_id
):
    source_dir = pathlib.Path(store_folder)

    if not source_dir.exists():
        LOGGER.error(f"{source_dir} does not exists")
        return 1

    with get_api_client()[0] as api_client:

        api_run = RunApi(api_client)
        _s = Store()
        for table_name in _s.list_tables():
            LOGGER.info(f"Sending data to table CD_{table_name}")
            data = convert_table_as_pylist(table_name)
            if not len(data):
                LOGGER.info("  - No rows : skipping")
                continue
            fieldnames = _s.get_table_schema(table_name).names
            for row in data:
                for field in fieldnames:
                    if row[field] is None:
                        del row[field]
            LOGGER.debug(f"  - Column list: {fieldnames}")
            LOGGER.info(f"  - Sending {len(data)} rows")
            api_run.send_run_data(organization_id,
                                  workspace_id,
                                  runner_id,
                                  run_id,
                                  SendRunDataRequest(id=table_name,
                                                     data=data))
