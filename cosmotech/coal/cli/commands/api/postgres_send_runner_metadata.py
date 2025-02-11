# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from adbc_driver_postgresql import dbapi

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.run import get_run_metadata
from cosmotech.coal.cosmotech_api.runner import get_runner_metadata
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.coal.utils.postgresql import generate_postgresql_full_uri


@click.command()
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
@click.option("--table-prefix",
              help="Prefix to add to the table name",
              metavar="PREFIX",
              type=str,
              default="Cosmotech_")
@click.option('--postgres-host',
              help='Postgresql host URI',
              envvar="POSTGRES_HOST_URI",
              show_envvar=True,
              required=True)
@click.option('--postgres-port',
              help='Postgresql database port',
              envvar="POSTGRES_HOST_PORT",
              show_envvar=True,
              required=False,
              default=5432)
@click.option('--postgres-db',
              help='Postgresql database name',
              envvar="POSTGRES_DB_NAME",
              show_envvar=True,
              required=True)
@click.option('--postgres-schema',
              help='Postgresql schema name',
              envvar="POSTGRES_DB_SCHEMA",
              show_envvar=True,
              required=True)
@click.option('--postgres-user',
              help='Postgresql connection user name',
              envvar="POSTGRES_USER_NAME",
              show_envvar=True,
              required=True)
@click.option('--postgres-password',
              help='Postgresql connection password',
              envvar="POSTGRES_USER_PASSWORD",
              show_envvar=True,
              required=True)
def postgres_send_runner_metadata(
    organization_id,
    workspace_id,
    runner_id,
    table_prefix: str,
    postgres_host,
    postgres_port,
    postgres_db,
    postgres_schema,
    postgres_user,
    postgres_password
):
    """Send a file to a workspace inside the API

Requires a valid connection to the API to send the data

This implementation make use of an API Key
    """

    with get_api_client()[0] as api_client:
        runner = get_runner_metadata(api_client, organization_id, workspace_id, runner_id)

    postgresql_full_uri = generate_postgresql_full_uri(postgres_host,
                                                       postgres_port,
                                                       postgres_db,
                                                       postgres_user,
                                                       postgres_password)

    with dbapi.connect(postgresql_full_uri, autocommit=True) as conn:
        with conn.cursor() as curs:
            schema_table = f"{postgres_schema}.{table_prefix}RunnerMetadata"
            sql_create_table = f"""
                CREATE TABLE IF NOT EXISTS {schema_table}  (
                  id varchar(32) PRIMARY KEY,
                  name varchar(256),
                  last_run_id varchar(32),
                  run_template_id varchar(32)
                );
            """
            sql_upsert = f"""
                INSERT INTO {schema_table} (id, name, last_run_id, run_template_id)
                  VALUES(%s, %s, %s, %s)
                  ON CONFLICT (id)
                  DO
                    UPDATE SET name = EXCLUDED.name, last_run_id = EXCLUDED.last_run_id;
            """
            LOGGER.info(f"creating table {schema_table}")
            curs.execute(sql_create_table)
            conn.commit()
            LOGGER.info(f"adding/updating runner metadata")
            curs.execute(
                sql_upsert,
                (
                    runner.get("id"),
                    runner.get("name"),
                    runner.get("lastRunId"),
                    runner.get("runTemplateId"),
                ),
            )
            conn.commit()
            LOGGER.info("Runner metadata table has been updated")
