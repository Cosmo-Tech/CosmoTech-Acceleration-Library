# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import json
import pathlib
from csv import DictWriter

from cosmotech_api import DatasetApi
from cosmotech_api import DatasetTwinGraphQuery
from cosmotech_api import RunnerApi
from cosmotech_api import ScenarioApi

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER


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

    runner_info = None
    if scenario_id:
        if runner_id:
            LOGGER.info(f"Both scenario ({scenario_id}) and runner ({runner_id}) id are defined; defaulting to scenario.")
        scenario_api = ScenarioApi(api_client)
        runner_info = scenario_api.find_scenario_by_id(
            organization_id,
            workspace_id,
            scenario_id,
        )
    elif runner_id:
        api_runner = RunnerApi(api_client)
        runner_info = api_runner.get_runner(
            organization_id,
            workspace_id,
            runner_id,
        )
    else:
        LOGGER.error(f"Neither scenario nor runner id is defined.")
        raise click.Abort()

    if len(runner_info.dataset_list) != 1:
        LOGGER.error(f"Runner {runner_id} is not tied to a single dataset")
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
            headers.remove("id")
            if "src" in headers:
                headers.remove("src")
                headers.remove("dest")
                headers = ["id", "src", "dest"] + sorted(headers)
            else:
                headers = ["id"] + sorted(headers)

            dw = DictWriter(_f,
                            fieldnames=headers)
            dw.writeheader()
            for row in sorted(files_content[file_name], key=lambda r: r.get('id')):
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
