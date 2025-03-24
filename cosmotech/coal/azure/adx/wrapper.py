# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from typing import Union, Optional, List, Dict, Iterator, Tuple, Any

from cosmotech.coal.azure.adx.auth import (
    create_kusto_client,
    create_ingest_client,
    get_cluster_urls,
)
from cosmotech.coal.azure.adx.query import run_query, run_command_query
from cosmotech.coal.azure.adx.ingestion import (
    ingest_dataframe,
    send_to_adx,
    check_ingestion_status,
    IngestionStatus,
)
from cosmotech.coal.azure.adx.tables import table_exists, create_table
from cosmotech.coal.azure.adx.utils import type_mapping


class ADXQueriesWrapper:
    """
    Wrapping class to ADX that uses modular functions from the adx package.
    This class maintains backward compatibility with the original implementation.
    """

    def __init__(
        self,
        database: str,
        cluster_url: Union[str, None] = None,
        ingest_url: Union[str, None] = None,
        cluster_name: Union[str, None] = None,
        cluster_region: Union[str, None] = None,
    ):
        """
        Initialize the ADXQueriesWrapper.

        Args:
            database: The name of the database
            cluster_url: The URL of the ADX cluster
            ingest_url: The ingestion URL of the ADX cluster
            cluster_name: The name of the ADX cluster
            cluster_region: The region of the ADX cluster
        """
        if cluster_name and cluster_region:
            cluster_url, ingest_url = get_cluster_urls(cluster_name, cluster_region)

        self.kusto_client = create_kusto_client(cluster_url)
        self.ingest_client = create_ingest_client(ingest_url)
        self.database = database
        self.timeout = 900

    def type_mapping(self, key: str, key_example_value: Any) -> str:
        """
        Map Python types to ADX types.

        Args:
            key: The name of the key
            key_example_value: A possible value of the key

        Returns:
            str: The name of the type used in ADX
        """
        return type_mapping(key, key_example_value)

    def send_to_adx(
        self,
        dict_list: list,
        table_name: str,
        ignore_table_creation: bool = True,
        drop_by_tag: str = None,
    ) -> Dict[str, str]:
        """
        Send a list of dictionaries to an ADX table.

        Args:
            dict_list: The list of dictionaries to send
            table_name: The name of the table
            ignore_table_creation: If False, will create the table if it doesn't exist
            drop_by_tag: Tag used for the drop by capacity of the Cosmotech API

        Returns:
            The ingestion result with source_id for status tracking
        """
        return send_to_adx(
            self.kusto_client,
            self.ingest_client,
            self.database,
            dict_list,
            table_name,
            ignore_table_creation,
            drop_by_tag,
        )

    def ingest_dataframe(self, table_name: str, dataframe: Any, drop_by_tag: str = None) -> Dict[str, str]:
        """
        Ingest a pandas DataFrame into an ADX table.

        Args:
            table_name: The name of the table
            dataframe: The DataFrame to ingest
            drop_by_tag: Tag used for the drop by capacity of the Cosmotech API

        Returns:
            The ingestion result with source_id for status tracking
        """
        return ingest_dataframe(self.ingest_client, self.database, table_name, dataframe, drop_by_tag)

    def check_ingestion_status(
        self, source_ids: List[str], timeout: int = None, logs: bool = False
    ) -> Iterator[Tuple[str, IngestionStatus]]:
        """
        Check the status of ingestion operations.

        Args:
            source_ids: List of source IDs to check
            timeout: Timeout in seconds (default: self.timeout)
            logs: Whether to log detailed information

        Returns:
            Iterator of (source_id, status) tuples
        """
        return check_ingestion_status(self.ingest_client, source_ids, timeout or self.timeout, logs)

    def _clear_ingestion_status_queues(self, confirmation: bool = False):
        """
        Clear all data in the ingestion status queues.
        DANGEROUS: This will clear all queues for the entire ADX cluster.

        Args:
            confirmation: Must be True to proceed with clearing
        """
        from cosmotech.coal.azure.adx.ingestion import clear_ingestion_status_queues

        clear_ingestion_status_queues(self.ingest_client, confirmation)

    def run_command_query(self, query: str) -> "KustoResponseDataSet":
        """
        Execute a command query on the database.

        Args:
            query: The query to execute

        Returns:
            KustoResponseDataSet: The results of the query
        """
        return run_command_query(self.kusto_client, self.database, query)

    def run_query(self, query: str) -> "KustoResponseDataSet":
        """
        Execute a simple query on the database.

        Args:
            query: The query to execute

        Returns:
            KustoResponseDataSet: The results of the query
        """
        return run_query(self.kusto_client, self.database, query)

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: The name of the table to check

        Returns:
            bool: True if the table exists, False otherwise
        """
        return table_exists(self.kusto_client, self.database, table_name)

    def create_table(self, table_name: str, schema: dict) -> bool:
        """
        Create a table in the database.

        Args:
            table_name: The name of the table to create
            schema: Dictionary mapping column names to ADX types

        Returns:
            bool: True if the table was created successfully, False otherwise
        """
        return create_table(self.kusto_client, self.database, table_name, schema)
