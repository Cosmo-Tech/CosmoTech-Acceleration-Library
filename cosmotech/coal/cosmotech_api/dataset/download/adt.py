# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import time
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional, Union, Tuple

from azure.digitaltwins.core import DigitalTwinsClient
from azure.identity import DefaultAzureCredential

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.dataset.converters import convert_dataset_to_files


def download_adt_dataset(
    adt_address: str,
    target_folder: Optional[Union[str, Path]] = None,
    credentials: Optional[DefaultAzureCredential] = None,
) -> Tuple[Dict[str, Any], Path]:
    """
    Download dataset from Azure Digital Twins.

    Args:
        adt_address: The ADT instance address
        target_folder: Optional folder to save files (if None, uses temp dir)
        credentials: Optional Azure credentials (if None, uses DefaultAzureCredential)

    Returns:
        Tuple of (content dict, folder path)
    """
    start_time = time.time()
    LOGGER.info(T("coal.services.dataset.download_started").format(dataset_type="ADT"))
    LOGGER.debug(T("coal.services.dataset.adt_connecting").format(url=adt_address))

    # Create credentials if not provided
    if credentials is None:
        if get_api_client()[1] == "Azure Entra Connection":
            credentials = DefaultAzureCredential()
        else:
            LOGGER.error(T("coal.services.dataset.adt_no_credentials"))
            raise ValueError("No credentials available for ADT connection")

    # Create client and download data
    client = DigitalTwinsClient(adt_address, credentials)

    # Query twins
    query_start = time.time()
    LOGGER.debug(T("coal.services.dataset.adt_querying_twins"))
    query_expression = "SELECT * FROM digitaltwins"
    query_result = client.query_twins(query_expression)

    json_content = dict()
    twin_count = 0

    for twin in query_result:
        twin_count += 1
        entity_type = twin.get("$metadata").get("$model").split(":")[-1].split(";")[0]
        t_content = {k: v for k, v in twin.items()}
        t_content["id"] = t_content["$dtId"]

        # Remove system properties
        for k in list(twin.keys()):
            if k[0] == "$":
                del t_content[k]

        json_content.setdefault(entity_type, [])
        json_content[entity_type].append(t_content)

    query_time = time.time() - query_start
    LOGGER.debug(T("coal.services.dataset.adt_twins_found").format(count=twin_count))
    LOGGER.debug(T("coal.common.timing.operation_completed").format(operation="twins query", time=query_time))

    # Query relationships
    rel_start = time.time()
    LOGGER.debug(T("coal.services.dataset.adt_querying_relations"))
    relations_query = "SELECT * FROM relationships"
    query_result = client.query_twins(relations_query)

    relation_count = 0
    for relation in query_result:
        relation_count += 1
        tr = {"$relationshipId": "id", "$sourceId": "source", "$targetId": "target"}
        r_content = {k: v for k, v in relation.items()}

        # Map system properties to standard names
        for k, v in tr.items():
            r_content[v] = r_content[k]

        # Remove system properties
        for k in list(relation.keys()):
            if k[0] == "$":
                del r_content[k]

        json_content.setdefault(relation["$relationshipName"], [])
        json_content[relation["$relationshipName"]].append(r_content)

    rel_time = time.time() - rel_start
    LOGGER.debug(T("coal.services.dataset.adt_relations_found").format(count=relation_count))
    LOGGER.debug(T("coal.common.timing.operation_completed").format(operation="relations query", time=rel_time))

    # Convert to files if target_folder is provided
    if target_folder:
        dataset_info = {"type": "adt", "content": json_content, "name": "ADT Dataset"}
        target_folder = convert_dataset_to_files(dataset_info, target_folder)
    else:
        target_folder = tempfile.mkdtemp()

    elapsed_time = time.time() - start_time
    LOGGER.info(T("coal.common.timing.operation_completed").format(operation="ADT download", time=elapsed_time))
    LOGGER.info(T("coal.services.dataset.download_completed").format(dataset_type="ADT"))

    return json_content, Path(target_folder)
