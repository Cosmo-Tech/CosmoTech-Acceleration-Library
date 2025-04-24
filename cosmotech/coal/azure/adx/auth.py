# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
from typing import Union, Optional, Tuple

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest import QueuedIngestClient

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def create_kusto_client(
    cluster_url: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    tenant_id: Optional[str] = None,
) -> KustoClient:
    """
    Create a KustoClient for querying ADX.

    Args:
        cluster_url: The URL of the ADX cluster
        client_id: Azure client ID (optional, will use environment variable if not provided)
        client_secret: Azure client secret (optional, will use environment variable if not provided)
        tenant_id: Azure tenant ID (optional, will use environment variable if not provided)

    Returns:
        KustoClient: A client for querying ADX
    """
    LOGGER.debug(T("coal.services.adx.creating_kusto_client").format(cluster_url=cluster_url))

    try:
        az_client_id = client_id or os.environ["AZURE_CLIENT_ID"]
        az_client_secret = client_secret or os.environ["AZURE_CLIENT_SECRET"]
        az_tenant_id = tenant_id or os.environ["AZURE_TENANT_ID"]

        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            cluster_url, az_client_id, az_client_secret, az_tenant_id
        )
        LOGGER.debug(T("coal.services.adx.using_app_auth"))
    except KeyError:
        LOGGER.debug(T("coal.services.adx.using_cli_auth"))
        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_url)

    return KustoClient(kcsb)


def create_ingest_client(
    ingest_url: str,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    tenant_id: Optional[str] = None,
) -> QueuedIngestClient:
    """
    Create a QueuedIngestClient for ingesting data to ADX.

    Args:
        ingest_url: The ingestion URL of the ADX cluster
        client_id: Azure client ID (optional, will use environment variable if not provided)
        client_secret: Azure client secret (optional, will use environment variable if not provided)
        tenant_id: Azure tenant ID (optional, will use environment variable if not provided)

    Returns:
        QueuedIngestClient: A client for ingesting data to ADX
    """
    LOGGER.debug(T("coal.services.adx.creating_ingest_client").format(ingest_url=ingest_url))

    try:
        az_client_id = client_id or os.environ["AZURE_CLIENT_ID"]
        az_client_secret = client_secret or os.environ["AZURE_CLIENT_SECRET"]
        az_tenant_id = tenant_id or os.environ["AZURE_TENANT_ID"]

        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            ingest_url, az_client_id, az_client_secret, az_tenant_id
        )
        LOGGER.debug(T("coal.services.adx.using_app_auth"))
    except KeyError:
        LOGGER.debug(T("coal.services.adx.using_cli_auth"))
        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(ingest_url)

    return QueuedIngestClient(kcsb)


def initialize_clients(adx_uri: str, adx_ingest_uri: str) -> Tuple[KustoClient, QueuedIngestClient]:
    """
    Initialize and return the Kusto and ingest clients.

    Args:
        adx_uri: The Azure Data Explorer resource URI
        adx_ingest_uri: The Azure Data Explorer resource ingest URI

    Returns:
        tuple: (kusto_client, ingest_client)
    """
    LOGGER.debug(T("coal.services.adx.initializing_clients"))
    kusto_client = create_kusto_client(adx_uri)
    ingest_client = create_ingest_client(adx_ingest_uri)
    return kusto_client, ingest_client


def get_cluster_urls(cluster_name: str, cluster_region: str) -> Tuple[str, str]:
    """
    Generate cluster and ingest URLs from cluster name and region.

    Args:
        cluster_name: The name of the ADX cluster
        cluster_region: The region of the ADX cluster

    Returns:
        tuple: (cluster_url, ingest_url)
    """
    LOGGER.debug(
        T("coal.services.adx.generating_urls").format(cluster_name=cluster_name, cluster_region=cluster_region)
    )

    cluster_url = f"https://{cluster_name}.{cluster_region}.kusto.windows.net"
    ingest_url = f"https://ingest-{cluster_name}.{cluster_region}.kusto.windows.net"

    return cluster_url, ingest_url
