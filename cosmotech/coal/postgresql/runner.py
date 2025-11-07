# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
PostgreSQL runner operations module.

This module provides functions for interacting with PostgreSQL databases
for runner metadata operations.
"""

from adbc_driver_postgresql import dbapi
from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.runner.metadata import get_runner_metadata
from cosmotech.coal.postgresql.utils import PostgresUtils
from cosmotech.coal.utils.configuration import Configuration
from cosmotech.coal.utils.logger import LOGGER


def send_runner_metadata_to_postgresql(
    organization_id: str,
    workspace_id: str,
    runner_id: str,
    postgres_host: str,
    postgres_port: int,
    postgres_db: str,
    postgres_schema: str,
    postgres_user: str,
    postgres_password: str,
    table_prefix: str = "Cosmotech_",
    force_encode: bool = False,
) -> str:
    """
    Send runner metadata to a PostgreSQL database.

    Args:
        organization_id: Organization ID
        workspace_id: Workspace ID
        runner_id: Runner ID
        postgres_host: PostgreSQL host
        postgres_port: PostgreSQL port
        postgres_db: PostgreSQL database name
        postgres_schema: PostgreSQL schema
        postgres_user: PostgreSQL username
        postgres_password: PostgreSQL password
        table_prefix: Table prefix
        force_encode: force password encoding to percent encoding
    """
    _c = Configuration()
    _c.postgres.host = postgres_host
    _c.postgres.port = postgres_port
    _c.postgres.db_name = postgres_db
    _c.postgres.db_schema = postgres_schema
    _c.postgres.user_name = postgres_user
    _c.postgres.user_password = postgres_password
    _c.postgres.password_encoding = force_encode
    _c.postgres.table_prefix = table_prefix

    _c.cosmotech.organization_id = organization_id
    _c.cosmotech.workspace_id = workspace_id
    _c.cosmotech.runner_id = runner_id

    return send_runner_metadata_to_postgresql_from_conf(_c)


def send_runner_metadata_to_postgresql_from_conf(
    configuration: Configuration,
) -> str:
    """
    Send runner metadata to a PostgreSQL database.

    Args:
        configuration: coal configuration
        organization_id: Organization ID
        workspace_id: Workspace ID
        runner_id: Runner ID
    """
    _psql = PostgresUtils(configuration)

    # Get runner metadata
    with get_api_client()[0] as api_client:
        runner = get_runner_metadata(
            api_client,
            configuration.cosmotech.organization_id,
            configuration.cosmotech.workspace_id,
            configuration.cosmotech.runner_id,
        )

    # Connect to PostgreSQL and update runner metadata
    with dbapi.connect(_psql.full_uri, autocommit=True) as conn:
        with conn.cursor() as curs:
            schema_table = f"{_psql.db_schema}.{_psql.table_prefix}RunnerMetadata"
            sql_create_table = f"""
                CREATE TABLE IF NOT EXISTS {schema_table}  (
                  id varchar(32) PRIMARY KEY,
                  name varchar(256),
                  last_run_id varchar(32),
                  run_template_id varchar(32)
                );
            """
            LOGGER.info(T("coal.services.postgresql.creating_table").format(schema_table=schema_table))
            curs.execute(sql_create_table)
            conn.commit()
            LOGGER.info(T("coal.services.postgresql.metadata"))
            sql_upsert = f"""
                INSERT INTO {schema_table} (id, name, last_run_id, run_template_id)
                  VALUES(%s, %s, %s, %s)
                  ON CONFLICT (id)
                  DO
                    UPDATE SET name = EXCLUDED.name, last_run_id = EXCLUDED.last_run_id;
            """
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
            LOGGER.info(T("coal.services.postgresql.metadata_updated"))
    return runner.get("lastRunId")


def remove_runner_metadata_from_postgresql(
    organization_id: str,
    workspace_id: str,
    runner_id: str,
    postgres_host: str,
    postgres_port: int,
    postgres_db: str,
    postgres_schema: str,
    postgres_user: str,
    postgres_password: str,
    table_prefix: str = "Cosmotech_",
    force_encode: bool = False,
) -> str:
    """
    Removes run_id from metadata table that trigger cascade delete on other tables

    Args:
        organization_id: Organization ID
        workspace_id: Workspace ID
        runner_id: Runner ID
        postgres_host: PostgreSQL host
        postgres_port: PostgreSQL port
        postgres_db: PostgreSQL database name
        postgres_schema: PostgreSQL schema
        postgres_user: PostgreSQL username
        postgres_password: PostgreSQL password
        table_prefix: Table prefix
        force_encode: force password encoding to percent encoding
    """
    _c = Configuration()
    _c.postgres.host = postgres_host
    _c.postgres.port = postgres_port
    _c.postgres.db_name = postgres_db
    _c.postgres.db_schema = postgres_schema
    _c.postgres.user_name = postgres_user
    _c.postgres.user_password = postgres_password
    _c.postgres.password_encoding = force_encode
    _c.postgres.table_prefix = table_prefix

    _c.cosmotech.organization_id = organization_id
    _c.cosmotech.workspace_id = workspace_id
    _c.cosmotech.runner_id = runner_id

    return remove_runner_metadata_from_postgresql_from_conf(_c)


def remove_runner_metadata_from_postgresql_from_conf(
    configuration: Configuration,
) -> str:
    """
    Removes run_id from metadata table that trigger cascade delete on other tables

    Args:
        configuration: coal configuration
        organization_id: Organization ID
        workspace_id: Workspace ID
        runner_id: Runner ID
    """
    _psql = PostgresUtils(configuration)

    # Get runner metadata
    with get_api_client()[0] as api_client:
        runner = get_runner_metadata(
            api_client,
            configuration.cosmotech.organization_id,
            configuration.cosmotech.workspace_id,
            configuration.cosmotech.runner_id,
        )

    # Connect to PostgreSQL and remove runner metadata row
    with dbapi.connect(_psql.full_uri, autocommit=True) as conn:
        with conn.cursor() as curs:
            schema_table = f"{_psql.db_schema}.{_psql.table_prefix}RunnerMetadata"
            sql_delete_from_metatable = f"""
                DELETE FROM {schema_table}
                WHERE last_run_id={runner.get("lastRunId")};
            """
            curs.execute(sql_delete_from_metatable)
            conn.commit()
    return runner.get("lastRunId")
