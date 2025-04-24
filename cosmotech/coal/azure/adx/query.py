# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from azure.kusto.data import KustoClient
from azure.kusto.data.response import KustoResponseDataSet

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def run_query(client: KustoClient, database: str, query: str) -> KustoResponseDataSet:
    """
    Execute a simple query on the database.

    Args:
        client: The KustoClient to use
        database: The name of the database
        query: The query to execute

    Returns:
        KustoResponseDataSet: The results of the query
    """
    LOGGER.debug(T("coal.services.adx.running_query").format(database=database, query=query))

    result = client.execute(database, query)
    LOGGER.debug(
        T("coal.services.adx.query_complete").format(
            rows=len(result.primary_results[0]) if result.primary_results else 0
        )
    )

    return result


def run_command_query(client: KustoClient, database: str, query: str) -> KustoResponseDataSet:
    """
    Execute a command query on the database.

    Args:
        client: The KustoClient to use
        database: The name of the database
        query: The query to execute

    Returns:
        KustoResponseDataSet: The results of the query
    """
    LOGGER.debug(T("coal.services.adx.running_command").format(database=database, query=query))

    result = client.execute_mgmt(database, query)
    LOGGER.debug(T("coal.services.adx.command_complete"))

    return result
