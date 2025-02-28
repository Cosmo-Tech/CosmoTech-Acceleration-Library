# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from typing import Dict

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
    LOGGER.debug(T("coal.logs.adx.checking_table").format(database=database, table_name=table_name))

    get_tables_query = f".show database ['{database}'] schema| distinct TableName"
    tables = client.execute(database, get_tables_query)

    for r in tables.primary_results[0]:
        if table_name == r[0]:
            LOGGER.debug(T("coal.logs.adx.table_exists").format(table_name=table_name))
            return True

    LOGGER.debug(T("coal.logs.adx.table_not_exists").format(table_name=table_name))
    return False


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
    LOGGER.debug(T("coal.logs.adx.creating_table").format(database=database, table_name=table_name))

    create_query = f".create-merge table {table_name}("

    for column_name, column_type in schema.items():
        create_query += f"{column_name}:{column_type},"

    create_query = create_query[:-1] + ")"

    LOGGER.debug(T("coal.logs.adx.create_query").format(query=create_query))

    try:
        client.execute(database, create_query)
        LOGGER.info(T("coal.logs.adx.table_created").format(table_name=table_name))
        return True
    except Exception as e:
        LOGGER.error(T("coal.logs.adx.table_creation_error").format(table_name=table_name, error=str(e)))
        return False
