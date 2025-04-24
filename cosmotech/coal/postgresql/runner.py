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

from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.runner.metadata import get_runner_metadata
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.coal.utils.postgresql import generate_postgresql_full_uri
from cosmotech.orchestrator.utils.translate import T


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
) -> None:
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
    """
    # Get runner metadata
    with get_api_client()[0] as api_client:
        runner = get_runner_metadata(api_client, organization_id, workspace_id, runner_id)

    # Generate PostgreSQL URI
    postgresql_full_uri = generate_postgresql_full_uri(
        postgres_host, str(postgres_port), postgres_db, postgres_user, postgres_password
    )

    # Connect to PostgreSQL and update runner metadata
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
            LOGGER.info(T("coal.services.postgresql.creating_table").format(schema_table=schema_table))
            curs.execute(sql_create_table)
            conn.commit()
            LOGGER.info(T("coal.services.postgresql.metadata"))
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
