# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
import psycopg2
import psycopg2.extras
import json
import glob
from csv import DictReader
import pathlib
from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@click.option("--output-absolute-path",
              envvar="CSM_OUTPUT_ABSOLUTE_PATH",
              show_envvar=True,
              help="A local folder to store the main output content",
              metavar="PATH",
              required=True)
@click.option("--postgres-host",
              envvar="CSM_POSTGRES_HOST",
              show_envvar=True,
              required=True,
              metavar="URI",
              help="The postgres host URI")
@click.option("--postgres-port",
              envvar="CSM_POSTGRES_PORT",
              show_envvar=True,
              required=False,
              default=5432,
              metavar="INT",
              help="The postgres host port")
@click.option("--postgres-db",
              envvar="CSM_POSTGRES_DB",
              show_envvar=True,
              required=True,
              metavar="DATABASE_NAME",
              help="The postgres database name")
@click.option("--postgres-schema",
              envvar="CSM_POSTGRES_SCHEMA",
              show_envvar=True,
              required=True,
              metavar="SCHEMA_NAME",
              help="The postgres database schema name")
@click.option("--postgres-user",
              envvar="CSM_POSTGRES_USER",
              show_envvar=True,
              required=True,
              metavar="USER_NAME",
              help="The postgres connection user name")
@click.option("--postgres-password",
              envvar="CSM_POSTGRES_SCHEMA",
              show_envvar=True,
              required=True,
              metavar="USER_PASSWORD",
              help="The postgres connection user password")
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              show_envvar=True,
              required=True,
              metavar="STRING",
              help="The Cosmo Tech o-xxxx organization ID")
@click.option("--workspace-id",
              envvar="CSM_WORKSPACE_ID",
              show_envvar=True,
              required=True,
              metavar="STRING",
              help="The Cosmo Tech w-xxxx workspace ID")
@click.option("--runner-id",
              envvar="CSM_RUNNER_ID",
              show_envvar=True,
              required=True,
              metavar="STRING",
              help="The Cosmo Tech r-xxxx runner ID")
@click.option("--run-id",
              envvar="CSM_RUN_ID",
              show_envvar=True,
              required=True,
              metavar="STRING",
              help="The Cosmo Tech run-xxxx run ID")
@web_help("csm-data/postgres-send-csv")
def postgres_send_csv(
    output_absolute_path: str,
    postgres_host: str,
    postgres_port: int,
    postgres_db: str,
    postgres_schema: str,
    postgres_user: str,
    postgres_password: str,
    organization_id: str,
    workspace_id: str,
    runner_id: str,
    run_id: str
):
    """
Send content of CSV files to a Postgres database with unique table formating and automatic schema detection.
    """
    LOGGER.info("Sending CSV files to Postgres")


if __name__ == "__main__":
    postgres_send_csv()
