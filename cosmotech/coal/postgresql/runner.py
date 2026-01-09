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

from cosmotech.coal.cosmotech_api.apis.runner import RunnerApi
from cosmotech.coal.postgresql.utils import PostgresUtils
from cosmotech.coal.utils.configuration import Configuration
from cosmotech.coal.utils.logger import LOGGER


def send_runner_metadata_to_postgresql(
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
    _runner_api = RunnerApi(configuration)
    runner = _runner_api.get_runner_metadata(
        configuration.cosmotech.runner_id,
    )

    # Connect to PostgreSQL and update runner metadata
    with dbapi.connect(_psql.full_uri, autocommit=True) as conn:
        with conn.cursor() as curs:
            schema_table = f"{str(_psql.db_schema)}.{str(_psql.table_prefix)}RunnerMetadata"
            sql_create_table = f"""
                CREATE TABLE IF NOT EXISTS {schema_table}  (
                  id varchar(32) PRIMARY KEY,
                  name varchar(256),
                  last_csm_run_id varchar(32),
                  run_template_id varchar(32)
                );
            """
            LOGGER.info(T("coal.services.postgresql.creating_table").format(schema_table=schema_table))
            curs.execute(sql_create_table)
            conn.commit()
            LOGGER.info(T("coal.services.postgresql.metadata"))
            sql_upsert = f"""
                INSERT INTO {schema_table} (id, name, last_csm_run_id, run_template_id)
                  VALUES ($1, $2, $3, $4)
                  ON CONFLICT (id)
                  DO
                    UPDATE SET name = EXCLUDED.name, last_csm_run_id = EXCLUDED.last_csm_run_id;
            """
            LOGGER.info(runner)
            curs.execute(
                sql_upsert,
                (
                    runner.get("id"),
                    runner.get("name"),
                    runner.get("lastRunInfo").get("lastRunId"),
                    runner.get("runTemplateId"),
                ),
            )
            conn.commit()
            LOGGER.info(T("coal.services.postgresql.metadata_updated"))
    return runner.get("lastRunInfo").get("lastRunId")


def remove_runner_metadata_from_postgresql(
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
    _runner_api = RunnerApi(configuration)
    runner = _runner_api.get_runner_metadata(
        configuration.cosmotech.runner_id,
    )

    # Connect to PostgreSQL and remove runner metadata row
    with dbapi.connect(_psql.full_uri, autocommit=True) as conn:
        with conn.cursor() as curs:
            schema_table = f"{_psql.db_schema}.{_psql.table_prefix}RunnerMetadata"
            sql_delete_from_metatable = f"""
                DELETE FROM {schema_table}
                WHERE last_csm_run_id={runner.get("lastRunId")};
            """
            curs.execute(sql_delete_from_metatable)
            conn.commit()
    return runner.get("lastRunId")
