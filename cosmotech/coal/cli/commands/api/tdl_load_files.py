# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import json
import pathlib
from csv import DictWriter

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER
from cosmotech_api import DatasetApi
from cosmotech_api import DatasetTwinGraphQuery
from cosmotech_api import RunnerApi
from cosmotech_api import ScenarioApi


@click.command()
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              help="An organization id for the Cosmo Tech API",
              metavar="o-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--workspace-id",
              envvar="CSM_WORKSPACE_ID",
              help="A workspace id for the Cosmo Tech API",
              metavar="w-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--scenario-id",
              envvar="CSM_SCENARIO_ID",
              help="A scenario id for the Cosmo Tech API",
              metavar="s-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=False)
@click.option("--runner-id",
              envvar="CSM_RUNNER_ID",
              help="A runner id for the Cosmo Tech API",
              metavar="r-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=False)
@click.option("--dir",
              "directory_path",
              help="Path to the directory to write the results to",
              metavar="PATH",
              default="./",
              type=str,
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              show_envvar=True,
              required=True)
@web_help("csm-data/api/tdl-load-file")
def tdl_load_files(
    organization_id,
    workspace_id,
    scenario_id,
    runner_id,
    directory_path
):
    """Query a twingraph and loads all the data from it

Will create 1 csv file per node type / relationship type

The twingraph must have been populated using the "tdl-send-files" command for this to work correctly

Requires a valid connection to the API to send the data
    """

    api_client, connection_type = get_api_client()
    api_ds = DatasetApi(api_client)
    api_runner = RunnerApi(api_client)
    api_scenario = ScenarioApi(api_client)

    if (scenario_id is None) == (runner_id is None):
        LOGGER.error('Requires a single Scenario ID or Runner ID to work.' +
                     f'{"Both" if runner_id else "None"} were defined.')
        raise click.Abort()

    if runner_id:
        runner_info = api_runner.get_runner(
            organization_id,
            workspace_id,
            runner_id,
        )
    else:
        runner_info = api_scenario.find_scenario_by_id(
            organization_id,
            workspace_id,
            scenario_id,
        )

    if (datasets_len := len(runner_info.dataset_list)) != 1:
        LOGGER.error(f"{runner_info.id} is not tied to a single dataset but {datasets_len}")
        LOGGER.debug(runner_info)
        raise click.Abort()

    dataset_id = runner_info.dataset_list[0]

    dataset_info = api_ds.find_dataset_by_id(organization_id,
                                             dataset_id)

    if dataset_info.ingestion_status != "SUCCESS":
        LOGGER.error(f"Dataset {dataset_id} is in state {dataset_info.ingestion_status}")
        LOGGER.debug(dataset_info)
        raise click.Abort()

    directory_path = pathlib.Path(directory_path)
    if directory_path.is_file():
        LOGGER.error(f"{directory_path} is not a directory.")
        raise click.Abort()

    directory_path.mkdir(parents=True, exist_ok=True)
    item_queries = dict()

    get_node_properties_query = "MATCH (n) RETURN distinct labels(n)[0] as label,  keys(n) as keys"
    node_properties_results: list[dict] = api_ds.twingraph_query(organization_id,
                                                                 dataset_id,
                                                                 DatasetTwinGraphQuery(query=get_node_properties_query))

    properties_nodes = dict()
    for _r in node_properties_results:
        label = _r["label"]
        keys = _r["keys"]
        if label not in properties_nodes:
            properties_nodes[label] = set()
        properties_nodes[label].update(keys)

    for label, keys in properties_nodes.items():
        node_query = f"MATCH (n:{label}) RETURN {', '.join(map(lambda k: f'n.{k} as {k}', keys))}"
        item_queries[label] = node_query

    get_relationship_properties_query = "MATCH ()-[r]->() RETURN distinct type(r) as label, keys(r) as keys"
    relationship_properties_results: list[dict] = api_ds.twingraph_query(organization_id,
                                                                         dataset_id,
                                                                         DatasetTwinGraphQuery(
                                                                             query=get_relationship_properties_query))

    properties_relationships = dict()
    for _r in relationship_properties_results:
        label = _r["label"]
        keys = _r["keys"]
        if label not in properties_relationships:
            properties_relationships[label] = set()
        properties_relationships[label].update(keys)

    for label, keys in properties_relationships.items():
        node_query = f"MATCH ()-[n:{label}]->() RETURN {', '.join(map(lambda k: f'n.{k} as {k}', keys))}"
        item_queries[label] = node_query

    files_content = dict()
    files_headers = dict()

    for element_type, query in item_queries.items():
        element_query: list[dict] = api_ds.twingraph_query(organization_id,
                                                           dataset_id,
                                                           DatasetTwinGraphQuery(query=query))
        for element in element_query:
            if element_type not in files_content:
                files_content[element_type] = list()
                files_headers[element_type] = set()
            files_content[element_type].append(element)
            files_headers[element_type].update(element.keys())

    for file_name in files_content.keys():
        file_path = directory_path / (file_name + ".csv")
        LOGGER.info(f"Writing {len(files_content[file_name])} lines in {file_path}")
        with (file_path.open("w") as _f):
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

            dw = DictWriter(_f,
                            fieldnames=headers)
            dw.writeheader()
            for row in sorted(files_content[file_name], key=lambda r: r.get('id',"")):
                dw.writerow({
                    key: (
                        json.dumps(value)
                        if isinstance(value, (bool, dict, list))
                        else value
                    )
                    for key, value
                    in row.items()
                })

    LOGGER.info("All CSV are written")
