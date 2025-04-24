# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from typing import Dict, List, Any

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def get_content_from_twin_graph_data(
    nodes: List[Dict], relationships: List[Dict], restore_names: bool = False
) -> Dict[str, List[Dict]]:
    """
    Extract content from twin graph data.

    When restore_names is True, the "id" value inside the "properties" field in the cypher query response is used
    instead of the numerical id found in the "id" field. When restore_names is set to False, this function
    keeps the previous behavior implemented when adding support for twingraph in v2 (default: False)

    Example with a sample of cypher response:
    [{
      n: {
        id: "50"  <-- this id is used if restore_names is False
        label: "Customer"
        properties: {
          Satisfaction: 0
          SurroundingSatisfaction: 0
          Thirsty: false
          id: "Lars_Coret"  <-- this id is used if restore_names is True
        }
        type: "NODE"
      }
    }]

    Args:
        nodes: List of node data from cypher query
        relationships: List of relationship data from cypher query
        restore_names: Whether to use property ID instead of node ID

    Returns:
        Dict mapping entity types to lists of entities
    """
    LOGGER.debug(
        T("coal.services.dataset.processing_graph_data").format(
            nodes_count=len(nodes),
            relationships_count=len(relationships),
            restore_names=restore_names,
        )
    )

    content = dict()
    # build keys
    for item in relationships:
        content[item["src"]["label"]] = list()
        content[item["dest"]["label"]] = list()
        content[item["rel"]["label"]] = list()

    # Process nodes
    for item in nodes:
        label = item["n"]["label"]
        props = item["n"]["properties"].copy()  # Create a copy to avoid modifying the original
        if not restore_names:
            props.update({"id": item["n"]["id"]})
        content.setdefault(label, list())
        content[label].append(props)

    # Process relationships
    for item in relationships:
        src = item["src"]
        dest = item["dest"]
        rel = item["rel"]
        props = rel["properties"].copy()  # Create a copy to avoid modifying the original
        content[rel["label"]].append(
            {
                "id": rel["id"],
                "source": src["properties"]["id"] if restore_names else src["id"],
                "target": dest["properties"]["id"] if restore_names else dest["id"],
                **props,
            }
        )

    # Log the number of entities by type
    for entity_type, entities in content.items():
        LOGGER.debug(T("coal.services.dataset.entity_count").format(entity_type=entity_type, count=len(entities)))

    return content


def sheet_to_header(sheet_content: List[Dict]) -> List[str]:
    """
    Extract header fields from sheet content.

    Args:
        sheet_content: List of dictionaries representing sheet rows

    Returns:
        List of field names with id, source, and target fields first if present
    """
    LOGGER.debug(T("coal.services.dataset.extracting_headers").format(rows=len(sheet_content)))

    fieldnames = []
    has_src = False
    has_id = False

    for r in sheet_content:
        for k in r.keys():
            if k not in fieldnames:
                if k in ["source", "target"]:
                    has_src = True
                elif k == "id":
                    has_id = True
                else:
                    fieldnames.append(k)

    # Ensure source/target and id fields come first
    if has_src:
        fieldnames = ["source", "target"] + fieldnames
    if has_id:
        fieldnames = ["id"] + fieldnames

    LOGGER.debug(
        T("coal.services.dataset.headers_extracted").format(
            count=len(fieldnames),
            fields=", ".join(fieldnames[:5]) + ("..." if len(fieldnames) > 5 else ""),
        )
    )

    return fieldnames
