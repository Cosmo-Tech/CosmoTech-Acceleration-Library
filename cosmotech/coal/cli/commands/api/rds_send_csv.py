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

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help, translate_help
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


@click.command()
@click.option(
    "--source-folder",
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    help=T("coal-help.commands.api.rds_send_csv.parameters.source_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    help=T("coal-help.commands.api.rds_send_csv.parameters.organization_id"),
    metavar="o-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    help=T("coal-help.commands.api.rds_send_csv.parameters.workspace_id"),
    metavar="w-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--runner-id",
    envvar="CSM_RUNNER_ID",
    help=T("coal-help.commands.api.rds_send_csv.parameters.runner_id"),
    metavar="r-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--run-id",
    envvar="CSM_RUN_ID",
    help=T("coal-help.commands.api.rds_send_csv.parameters.run_id"),
    metavar="run-XXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@web_help("csm-data/api/rds-send-csv")
@translate_help("coal-help.commands.api.rds_send_csv.description")
def rds_send_csv(source_folder, organization_id, workspace_id, runner_id, run_id):
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
                LOGGER.info(f"Sending data to table CD_{table_name}")
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
                api_run.send_run_data(
                    organization_id,
                    workspace_id,
                    runner_id,
                    run_id,
                    SendRunDataRequest(id=table_name, data=data),
                )
