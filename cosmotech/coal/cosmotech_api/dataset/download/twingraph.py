# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import time
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple

from cosmotech_api import (
    DatasetApi,
    DatasetTwinGraphQuery,
    TwinGraphQuery,
    TwingraphApi,
)

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.dataset.utils import get_content_from_twin_graph_data
from cosmotech.coal.cosmotech_api.dataset.converters import convert_dataset_to_files


def download_twingraph_dataset(
    organization_id: str,
    dataset_id: str,
    target_folder: Optional[Union[str, Path]] = None,
) -> Tuple[Dict[str, Any], Path]:
    """
    Download dataset from TwinGraph.

    Args:
        organization_id: Organization ID
        dataset_id: Dataset ID
        target_folder: Optional folder to save files (if None, uses temp dir)

    Returns:
        Tuple of (content dict, folder path)
    """
    start_time = time.time()
    LOGGER.info(T("coal.services.dataset.download_started").format(dataset_type="TwinGraph"))
    LOGGER.debug(
        T("coal.services.dataset.twingraph_downloading").format(organization_id=organization_id, dataset_id=dataset_id)
    )

    with get_api_client()[0] as api_client:
        dataset_api = DatasetApi(api_client)

        # Query nodes
        nodes_start = time.time()
        LOGGER.debug(T("coal.services.dataset.twingraph_querying_nodes").format(dataset_id=dataset_id))
        nodes_query = DatasetTwinGraphQuery(query="MATCH(n) RETURN n")

        nodes = dataset_api.twingraph_query(
            organization_id=organization_id,
            dataset_id=dataset_id,
            dataset_twin_graph_query=nodes_query,
        )

        nodes_time = time.time() - nodes_start
        LOGGER.debug(T("coal.services.dataset.twingraph_nodes_found").format(count=len(nodes)))
        LOGGER.debug(T("coal.common.timing.operation_completed").format(operation="nodes query", time=nodes_time))

        # Query edges
        edges_start = time.time()
        LOGGER.debug(T("coal.services.dataset.twingraph_querying_edges").format(dataset_id=dataset_id))
        edges_query = DatasetTwinGraphQuery(query="MATCH(n)-[r]->(m) RETURN n as src, r as rel, m as dest")

        edges = dataset_api.twingraph_query(
            organization_id=organization_id,
            dataset_id=dataset_id,
            dataset_twin_graph_query=edges_query,
        )

        edges_time = time.time() - edges_start
        LOGGER.debug(T("coal.services.dataset.twingraph_edges_found").format(count=len(edges)))
        LOGGER.debug(T("coal.common.timing.operation_completed").format(operation="edges query", time=edges_time))

        # Process results
        process_start = time.time()
        content = get_content_from_twin_graph_data(nodes, edges, True)
        process_time = time.time() - process_start

        LOGGER.debug(T("coal.common.timing.operation_completed").format(operation="data processing", time=process_time))

    # Convert to files if target_folder is provided
    if target_folder:
        dataset_info = {
            "type": "twincache",
            "content": content,
            "name": f"TwinGraph Dataset {dataset_id}",
        }
        target_folder = convert_dataset_to_files(dataset_info, target_folder)
    else:
        target_folder = tempfile.mkdtemp()

    elapsed_time = time.time() - start_time
    LOGGER.info(T("coal.common.timing.operation_completed").format(operation="TwinGraph download", time=elapsed_time))
    LOGGER.info(T("coal.services.dataset.download_completed").format(dataset_type="TwinGraph"))

    return content, Path(target_folder)


def download_legacy_twingraph_dataset(
    organization_id: str,
    cache_name: str,
    target_folder: Optional[Union[str, Path]] = None,
) -> Tuple[Dict[str, Any], Path]:
    """
    Download dataset from legacy TwinGraph.

    Args:
        organization_id: Organization ID
        cache_name: Twin cache name
        target_folder: Optional folder to save files (if None, uses temp dir)

    Returns:
        Tuple of (content dict, folder path)
    """
    start_time = time.time()
    LOGGER.info(T("coal.services.dataset.download_started").format(dataset_type="Legacy TwinGraph"))
    LOGGER.debug(
        T("coal.services.dataset.legacy_twingraph_downloading").format(
            organization_id=organization_id, cache_name=cache_name
        )
    )

    with get_api_client()[0] as api_client:
        api_instance = TwingraphApi(api_client)

        # Query nodes
        nodes_start = time.time()
        LOGGER.debug(T("coal.services.dataset.legacy_twingraph_querying_nodes").format(cache_name=cache_name))
        _query_nodes = TwinGraphQuery(query="MATCH(n) RETURN n")

        nodes = api_instance.query(
            organization_id=organization_id,
            graph_id=cache_name,
            twin_graph_query=_query_nodes,
        )

        nodes_time = time.time() - nodes_start
        LOGGER.debug(T("coal.services.dataset.legacy_twingraph_nodes_found").format(count=len(nodes)))
        LOGGER.debug(T("coal.common.timing.operation_completed").format(operation="nodes query", time=nodes_time))

        # Query relationships
        rel_start = time.time()
        LOGGER.debug(T("coal.services.dataset.legacy_twingraph_querying_relations").format(cache_name=cache_name))
        _query_rel = TwinGraphQuery(query="MATCH(n)-[r]->(m) RETURN n as src, r as rel, m as dest")

        rel = api_instance.query(
            organization_id=organization_id,
            graph_id=cache_name,
            twin_graph_query=_query_rel,
        )

        rel_time = time.time() - rel_start
        LOGGER.debug(T("coal.services.dataset.legacy_twingraph_relations_found").format(count=len(rel)))
        LOGGER.debug(T("coal.common.timing.operation_completed").format(operation="relations query", time=rel_time))

        # Process results
        process_start = time.time()
        content = get_content_from_twin_graph_data(nodes, rel, False)
        process_time = time.time() - process_start

        LOGGER.debug(T("coal.common.timing.operation_completed").format(operation="data processing", time=process_time))

    # Convert to files if target_folder is provided
    if target_folder:
        dataset_info = {
            "type": "twincache",
            "content": content,
            "name": f"Legacy TwinGraph Dataset {cache_name}",
        }
        target_folder = convert_dataset_to_files(dataset_info, target_folder)
    else:
        target_folder = tempfile.mkdtemp()

    elapsed_time = time.time() - start_time
    LOGGER.info(
        T("coal.common.timing.operation_completed").format(operation="Legacy TwinGraph download", time=elapsed_time)
    )
    LOGGER.info(T("coal.services.dataset.download_completed").format(dataset_type="Legacy TwinGraph"))

    return content, Path(target_folder)
