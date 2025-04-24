# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from typing import Dict, Any

import pyarrow
from azure.kusto.data import KustoClient

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def table_exists(client: KustoClient, database: str, table_name: str) -> bool:
    """
    Check if a table exists in the database.

    Args:
        client: The KustoClient to use
        database: The name of the database
        table_name: The name of the table to check

    Returns:
        bool: True if the table exists, False otherwise
    """
    LOGGER.debug(T("coal.services.adx.checking_table").format(database=database, table_name=table_name))

    get_tables_query = f".show database ['{database}'] schema| distinct TableName"
    tables = client.execute(database, get_tables_query)

    for r in tables.primary_results[0]:
        if table_name == r[0]:
            LOGGER.debug(T("coal.services.adx.table_exists").format(table_name=table_name))
            return True

    LOGGER.debug(T("coal.services.adx.table_not_exists").format(table_name=table_name))
    return False


def check_and_create_table(kusto_client: KustoClient, database: str, table_name: str, data: pyarrow.Table) -> bool:
    """
    Check if a table exists and create it if it doesn't.

    Args:
        kusto_client: The Kusto client
        database: The database name
        table_name: The table name
        data: The PyArrow table data

    Returns:
        bool: True if the table was created, False if it already existed
    """
    LOGGER.debug(T("coal.services.adx.checking_table_exists"))
    if not table_exists(kusto_client, database, table_name):
        from cosmotech.coal.azure.adx.utils import create_column_mapping

        mapping = create_column_mapping(data)
        LOGGER.debug(T("coal.services.adx.creating_nonexistent_table"))
        create_table(kusto_client, database, table_name, mapping)
        return True
    return False


def _drop_by_tag(kusto_client: KustoClient, database: str, tag: str) -> None:
    """
    Drop all data with the specified tag.

    Args:
        kusto_client: The Kusto client
        database: The database name
        tag: The tag to drop data by
    """
    LOGGER.info(T("coal.services.adx.dropping_data_by_tag").format(tag=tag))

    try:
        # Execute the drop by tag command
        drop_command = f'.drop extents <| .show database extents where tags has "drop-by:{tag}"'
        kusto_client.execute_mgmt(database, drop_command)
        LOGGER.info(T("coal.services.adx.drop_completed"))
    except Exception as e:
        LOGGER.error(T("coal.services.adx.drop_error").format(error=str(e)))
        LOGGER.exception(T("coal.services.adx.drop_details"))


def create_table(client: KustoClient, database: str, table_name: str, schema: Dict[str, str]) -> bool:
    """
    Create a table in the database.

    Args:
        client: The KustoClient to use
        database: The name of the database
        table_name: The name of the table to create
        schema: Dictionary mapping column names to ADX types

    Returns:
        bool: True if the table was created successfully, False otherwise
    """
    LOGGER.debug(T("coal.services.adx.creating_table").format(database=database, table_name=table_name))

    create_query = f".create-merge table {table_name}("

    for column_name, column_type in schema.items():
        create_query += f"{column_name}:{column_type},"

    create_query = create_query[:-1] + ")"

    LOGGER.debug(T("coal.services.adx.create_query").format(query=create_query))

    try:
        client.execute(database, create_query)
        LOGGER.info(T("coal.services.adx.table_created").format(table_name=table_name))
        return True
    except Exception as e:
        LOGGER.error(T("coal.services.adx.table_creation_error").format(table_name=table_name, error=str(e)))
        return False
