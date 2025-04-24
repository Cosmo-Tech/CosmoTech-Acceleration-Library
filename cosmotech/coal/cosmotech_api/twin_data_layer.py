# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Twin Data Layer operations module.

This module provides functions for interacting with the Twin Data Layer,
including sending and loading files.
"""

import json
import pathlib
from csv import DictReader, DictWriter
from io import StringIO
from typing import Dict, List, Any, Optional, Set, Tuple

import requests
from cosmotech_api import DatasetApi, RunnerApi, DatasetTwinGraphQuery

from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T

ID_COLUMN = "id"

SOURCE_COLUMN = "src"

TARGET_COLUMN = "dest"

BATCH_SIZE_LIMIT = 10000


class CSVSourceFile:
    def __init__(self, file_path: pathlib.Path):
        self.file_path = file_path
        if not file_path.name.endswith(".csv"):
            raise ValueError(T("coal.common.validation.not_csv_file").format(file_path=file_path))
        with open(file_path) as _file:
            dr = DictReader(_file)
            self.fields = list(dr.fieldnames)
        self.object_type = file_path.name[:-4]

        self.id_column = None
        self.source_column = None
        self.target_column = None

        for _c in self.fields:
            if _c.lower() == ID_COLUMN:
                self.id_column = _c
            if _c.lower() == SOURCE_COLUMN:
                self.source_column = _c
            if _c.lower() == TARGET_COLUMN:
                self.target_column = _c

        has_id = self.id_column is not None
        has_source = self.source_column is not None
        has_target = self.target_column is not None

        is_relation = all([has_source, has_target])

        if not has_id and not is_relation:
            LOGGER.error(T("coal.common.validation.invalid_nodes_relations").format(file_path=file_path))
            LOGGER.error(T("coal.common.validation.node_requirements").format(id_column=ID_COLUMN))
            LOGGER.error(
                T("coal.common.validation.relationship_requirements").format(
                    id_column=ID_COLUMN,
                    source_column=SOURCE_COLUMN,
                    target_column=TARGET_COLUMN,
                )
            )
            raise ValueError(T("coal.common.validation.invalid_nodes_relations").format(file_path=file_path))

        self.is_node = has_id and not is_relation

        self.content_fields = {
            _f: _f for _f in self.fields if _f not in [self.id_column, self.source_column, self.target_column]
        }
        if has_id:
            self.content_fields[ID_COLUMN] = self.id_column
        if is_relation:
            self.content_fields[SOURCE_COLUMN] = self.source_column
            self.content_fields[TARGET_COLUMN] = self.target_column

    def reload(self, inplace: bool = False) -> "CSVSourceFile":
        if inplace:
            self.__init__(self.file_path)
            return self
        return CSVSourceFile(self.file_path)

    def generate_query_insert(self) -> str:
        """
        Read a CSV file headers and generate a CREATE cypher query
        :return: the Cypher query for CREATE
        """

        field_names = sorted(self.content_fields.keys(), key=len, reverse=True)

        if self.is_node:
            query = (
                "CREATE (:"
                + self.object_type
                + ", ".join(f"{property_name}: ${self.content_fields[property_name]}" for property_name in field_names)
                + "})"
            )
            # query = ("UNWIND $params AS params " +
            #          f"MERGE (n:{self.object_type}) " +
            #          "SET n += params")
        else:
            query = (
                "MATCH "
                + "(source {"
                + ID_COLUMN
                + ":$"
                + self.source_column
                + "}),\n"
                + "(target {"
                + ID_COLUMN
                + ":$"
                + self.target_column
                + "})\n"
                + "CREATE (source)-[rel:"
                + self.object_type
                + " {"
                + ", ".join(f"{property_name}: ${self.content_fields[property_name]}" for property_name in field_names)
                + "}"
                + "]->(target)\n"
            )
            # query = ("UNWIND $params AS params " +
            #          "MATCH (source {" + ID_COLUMN + ":params." + self.source_column + "})\n" +
            #          "MATCH (target {" + ID_COLUMN + ":params." + self.target_column + "})\n" +
            #          f"CREATE (from) - [rel:{self.object_type}]->(to)" +
            #          "SET rel += params")
        return query


def get_dataset_id_from_runner(organization_id: str, workspace_id: str, runner_id: str) -> str:
    """
    Get the dataset ID from a runner.

    Args:
        organization_id: Organization ID
        workspace_id: Workspace ID
        runner_id: Runner ID

    Returns:
        Dataset ID
    """
    api_client, _ = get_api_client()
    api_runner = RunnerApi(api_client)

    runner_info = api_runner.get_runner(
        organization_id,
        workspace_id,
        runner_id,
    )

    if (datasets_len := len(runner_info.dataset_list)) != 1:
        LOGGER.error(
            T("coal.cosmotech_api.runner.not_single_dataset").format(runner_id=runner_info.id, count=datasets_len)
        )
        LOGGER.debug(T("coal.cosmotech_api.runner.runner_info").format(info=runner_info))
        raise ValueError(f"Runner {runner_info.id} does not have exactly one dataset")

    return runner_info.dataset_list[0]


def send_files_to_tdl(
    api_url: str,
    organization_id: str,
    workspace_id: str,
    runner_id: str,
    directory_path: str,
    clear: bool = True,
) -> None:
    """
    Send CSV files to the Twin Data Layer.

    Args:
        api_url: API URL
        organization_id: Organization ID
        workspace_id: Workspace ID
        runner_id: Runner ID
        directory_path: Directory containing CSV files
        clear: Whether to clear the dataset before sending files
    """
    api_client, _ = get_api_client()
    api_ds = DatasetApi(api_client)

    # Get dataset ID from runner
    dataset_id = get_dataset_id_from_runner(organization_id, workspace_id, runner_id)

    # Get dataset info
    dataset_info = api_ds.find_dataset_by_id(organization_id, dataset_id)
    dataset_info.ingestion_status = "SUCCESS"
    api_ds.update_dataset(organization_id, dataset_id, dataset_info)

    # Process CSV files
    entities_queries = {}
    relation_queries = {}

    content_path = pathlib.Path(directory_path)
    if not content_path.is_dir():
        LOGGER.error(T("coal.common.file_operations.not_directory").format(target_dir=directory_path))
        raise ValueError(f"{directory_path} is not a directory")

    # Process CSV files
    for file_path in content_path.glob("*.csv"):
        _csv = CSVSourceFile(file_path)
        if _csv.is_node:
            LOGGER.info(T("coal.services.azure_storage.sending_content").format(file=file_path))
            entities_queries[file_path] = _csv.generate_query_insert()
        else:
            LOGGER.info(T("coal.services.azure_storage.sending_content").format(file=file_path))
            relation_queries[file_path] = _csv.generate_query_insert()

    # Prepare headers
    header = {
        "Accept": "application/json",
        "Content-Type": "text/csv",
        "User-Agent": "OpenAPI-Generator/1.0.0/python",
    }
    header.update(api_client.default_headers)

    for authtype, authinfo in api_ds.api_client.configuration.auth_settings().items():
        api_ds.api_client._apply_auth_params(header, None, None, None, None, authinfo)

    # Clear dataset if requested
    if clear:
        LOGGER.info(T("coal.services.azure_storage.clearing_content"))
        clear_query = "MATCH (n) DETACH DELETE n"
        api_ds.twingraph_query(organization_id, dataset_id, DatasetTwinGraphQuery(query=str(clear_query)))

    # Send files
    for query_dict in [entities_queries, relation_queries]:
        for file_path, query in query_dict.items():
            _process_csv_file(
                file_path=file_path,
                query=query,
                api_url=api_url,
                organization_id=organization_id,
                dataset_id=dataset_id,
                header=header,
            )

    LOGGER.info(T("coal.services.azure_storage.all_data_sent"))

    # Update dataset status
    dataset_info.ingestion_status = "SUCCESS"
    dataset_info.twincache_status = "FULL"
    api_ds.update_dataset(organization_id, dataset_id, dataset_info)


def _process_csv_file(
    file_path: pathlib.Path,
    query: str,
    api_url: str,
    organization_id: str,
    dataset_id: str,
    header: Dict[str, str],
) -> None:
    """
    Process a CSV file and send it to the Twin Data Layer.

    Args:
        file_path: Path to the CSV file
        query: Query to execute
        api_url: API URL
        organization_id: Organization ID
        dataset_id: Dataset ID
        header: HTTP headers
    """
    content = StringIO()
    size = 0
    batch = 1
    errors = []
    query_craft = api_url + f"/organizations/{organization_id}/datasets/{dataset_id}/batch?query={query}"
    LOGGER.info(T("coal.services.azure_storage.sending_content").format(file=file_path))

    with open(file_path, "r") as _f:
        dr = DictReader(_f)
        dw = DictWriter(content, fieldnames=sorted(dr.fieldnames, key=len, reverse=True))
        dw.writeheader()
        for row in dr:
            dw.writerow(row)
            size += 1
            if size > BATCH_SIZE_LIMIT:
                LOGGER.info(T("coal.services.azure_storage.row_batch").format(count=batch * BATCH_SIZE_LIMIT))
                batch += 1
                content.seek(0)
                post = requests.post(query_craft, data=content.read(), headers=header)
                post.raise_for_status()
                errors.extend(json.loads(post.content)["errors"])
                content = StringIO()
                dw = DictWriter(
                    content,
                    fieldnames=sorted(dr.fieldnames, key=len, reverse=True),
                )
                dw.writeheader()
                size = 0

    if size > 0:
        content.seek(0)
        post = requests.post(query_craft, data=content.read(), headers=header)
        post.raise_for_status()
        errors.extend(json.loads(post.content)["errors"])

    if len(errors):
        LOGGER.error(T("coal.services.azure_storage.import_errors").format(count=len(errors)))
        for _err in errors:
            LOGGER.error(T("coal.services.azure_storage.error_detail").format(error=str(_err)))
        raise ValueError(f"Error importing data from {file_path}")


def load_files_from_tdl(
    organization_id: str,
    workspace_id: str,
    directory_path: str,
    runner_id: str,
) -> None:
    """
    Load files from the Twin Data Layer.

    Args:
        organization_id: Organization ID
        workspace_id: Workspace ID
        directory_path: Directory to save files to
        runner_id: Runner ID
    """
    api_client, _ = get_api_client()
    api_ds = DatasetApi(api_client)

    # Get dataset ID from runner
    dataset_id = get_dataset_id_from_runner(organization_id, workspace_id, runner_id)

    # Get dataset info
    dataset_info = api_ds.find_dataset_by_id(organization_id, dataset_id)
    if dataset_info.ingestion_status != "SUCCESS":
        LOGGER.error(
            T("coal.cosmotech_api.runner.dataset_state").format(
                dataset_id=dataset_id, status=dataset_info.ingestion_status
            )
        )
        LOGGER.debug(T("coal.cosmotech_api.runner.dataset_info").format(info=dataset_info))
        raise ValueError(f"Dataset {dataset_id} is not in SUCCESS state")

    # Create directory
    directory_path = pathlib.Path(directory_path)
    if directory_path.is_file():
        LOGGER.error(T("coal.common.file_operations.not_directory").format(target_dir=directory_path))
        raise ValueError(f"{directory_path} is not a directory")

    directory_path.mkdir(parents=True, exist_ok=True)

    # Get node and relationship properties
    item_queries = {}
    properties_nodes = _get_node_properties(api_ds, organization_id, dataset_id)
    properties_relationships = _get_relationship_properties(api_ds, organization_id, dataset_id)

    # Create queries
    for label, keys in properties_nodes.items():
        node_query = f"MATCH (n:{label}) RETURN {', '.join(map(lambda k: f'n.`{k}` as `{k}`', keys))}"
        item_queries[label] = node_query

    for label, keys in properties_relationships.items():
        rel_query = f"MATCH ()-[n:{label}]->() RETURN {', '.join(map(lambda k: f'n.`{k}` as `{k}`', keys))}"
        item_queries[label] = rel_query

    # Execute queries and write files
    files_content, files_headers = _execute_queries(api_ds, organization_id, dataset_id, item_queries)
    _write_files(directory_path, files_content, files_headers)

    LOGGER.info(T("coal.services.azure_storage.all_csv_written"))


def _get_node_properties(api_ds: DatasetApi, organization_id: str, dataset_id: str) -> Dict[str, Set[str]]:
    """
    Get node properties from the Twin Data Layer.

    Args:
        api_ds: Dataset API
        organization_id: Organization ID
        dataset_id: Dataset ID

    Returns:
        Dictionary of node labels to sets of property keys
    """
    get_node_properties_query = "MATCH (n) RETURN distinct labels(n)[0] as label, keys(n) as keys"
    node_properties_results: List[Dict[str, Any]] = api_ds.twingraph_query(
        organization_id,
        dataset_id,
        DatasetTwinGraphQuery(query=get_node_properties_query),
    )

    properties_nodes = {}
    for _r in node_properties_results:
        label = _r["label"]
        keys = _r["keys"]
        if label not in properties_nodes:
            properties_nodes[label] = set()
        properties_nodes[label].update(keys)

    return properties_nodes


def _get_relationship_properties(api_ds: DatasetApi, organization_id: str, dataset_id: str) -> Dict[str, Set[str]]:
    """
    Get relationship properties from the Twin Data Layer.

    Args:
        api_ds: Dataset API
        organization_id: Organization ID
        dataset_id: Dataset ID

    Returns:
        Dictionary of relationship types to sets of property keys
    """
    get_relationship_properties_query = "MATCH ()-[r]->() RETURN distinct type(r) as label, keys(r) as keys"
    relationship_properties_results: List[Dict[str, Any]] = api_ds.twingraph_query(
        organization_id,
        dataset_id,
        DatasetTwinGraphQuery(query=get_relationship_properties_query),
    )

    properties_relationships = {}
    for _r in relationship_properties_results:
        label = _r["label"]
        keys = _r["keys"]
        if label not in properties_relationships:
            properties_relationships[label] = set()
        properties_relationships[label].update(keys)

    return properties_relationships


def _execute_queries(
    api_ds: DatasetApi, organization_id: str, dataset_id: str, item_queries: Dict[str, str]
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Set[str]]]:
    """
    Execute queries against the Twin Data Layer.

    Args:
        api_ds: Dataset API
        organization_id: Organization ID
        dataset_id: Dataset ID
        item_queries: Dictionary of element types to queries

    Returns:
        Tuple of (files_content, files_headers)
    """
    files_content = {}
    files_headers = {}

    for element_type, query in item_queries.items():
        element_query: List[Dict[str, Any]] = api_ds.twingraph_query(
            organization_id, dataset_id, DatasetTwinGraphQuery(query=query)
        )
        for element in element_query:
            if element_type not in files_content:
                files_content[element_type] = []
                files_headers[element_type] = set()
            files_content[element_type].append(element)
            files_headers[element_type].update(element.keys())

    return files_content, files_headers


def _write_files(
    directory_path: pathlib.Path,
    files_content: Dict[str, List[Dict[str, Any]]],
    files_headers: Dict[str, Set[str]],
) -> None:
    """
    Write files to disk.

    Args:
        directory_path: Directory to write files to
        files_content: Dictionary of file names to lists of rows
        files_headers: Dictionary of file names to sets of headers
    """
    for file_name in files_content.keys():
        file_path = directory_path / (file_name + ".csv")
        LOGGER.info(
            T("coal.services.azure_storage.writing_lines").format(count=len(files_content[file_name]), file=file_path)
        )
        with file_path.open("w") as _f:
            headers = files_headers[file_name]
            has_id = "id" in headers
            is_relation = "src" in headers
            new_headers = []
            if has_id:
                headers.remove("id")
                new_headers.append("id")
            if is_relation:
                headers.remove("src")
                headers.remove("dest")
                new_headers.append("src")
                new_headers.append("dest")
            headers = new_headers + sorted(headers)

            dw = DictWriter(_f, fieldnames=headers)
            dw.writeheader()
            for row in sorted(files_content[file_name], key=lambda r: r.get("id", "")):
                dw.writerow(
                    {
                        key: (json.dumps(value) if isinstance(value, (bool, dict, list)) else value)
                        for key, value in row.items()
                    }
                )
