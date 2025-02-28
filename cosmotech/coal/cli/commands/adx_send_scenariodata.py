# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help, translate_help
from cosmotech.orchestrator.utils.translate import T


@click.command()
@web_help("csm-data/adx-send-scenario-data")
@translate_help("coal-help.commands.storage.adx_send_scenariodata.description")
@click.option(
    "--dataset-absolute-path",
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    show_envvar=True,
    help=T("coal-help.commands.storage.adx_send_scenariodata.parameters.dataset_absolute_path"),
    metavar="PATH",
    required=True,
)
@click.option(
    "--parameters-absolute-path",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    metavar="PATH",
    show_envvar=True,
    help=T("coal-help.commands.storage.adx_send_scenariodata.parameters.parameters_absolute_path"),
    required=True,
)
@click.option(
    "--simulation-id",
    envvar="CSM_SIMULATION_ID",
    show_envvar=True,
    required=True,
    metavar="UUID",
    help=T("coal-help.commands.storage.adx_send_scenariodata.parameters.simulation_id"),
)
@click.option(
    "--adx-uri",
    envvar="AZURE_DATA_EXPLORER_RESOURCE_URI",
    show_envvar=True,
    required=True,
    metavar="URI",
    help=T("coal-help.commands.storage.adx_send_scenariodata.parameters.adx_uri"),
)
@click.option(
    "--adx-ingest-uri",
    envvar="AZURE_DATA_EXPLORER_RESOURCE_INGEST_URI",
    show_envvar=True,
    required=True,
    metavar="URI",
    help=T("coal-help.commands.storage.adx_send_scenariodata.parameters.adx_ingest_uri"),
)
@click.option(
    "--database-name",
    envvar="AZURE_DATA_EXPLORER_DATABASE_NAME",
    show_envvar=True,
    required=True,
    metavar="NAME",
    help=T("coal-help.commands.storage.adx_send_scenariodata.parameters.database_name"),
)
@click.option(
    "--send-parameters/--no-send-parameters",
    type=bool,
    envvar="CSM_SEND_DATAWAREHOUSE_PARAMETERS",
    show_envvar=True,
    default=False,
    show_default=True,
    help=T("coal-help.commands.storage.adx_send_scenariodata.parameters.send_parameters"),
)
@click.option(
    "--send-datasets/--no-send-datasets",
    type=bool,
    envvar="CSM_SEND_DATAWAREHOUSE_DATASETS",
    show_envvar=True,
    default=False,
    show_default=True,
    help=T("coal-help.commands.storage.adx_send_scenariodata.parameters.send_datasets"),
)
@click.option(
    "--wait/--no-wait",
    envvar="WAIT_FOR_INGESTION",
    show_envvar=True,
    default=False,
    show_default=True,
    help=T("coal-help.commands.storage.adx_send_scenariodata.parameters.wait"),
)
def adx_send_scenariodata(
    send_parameters: bool,
    send_datasets: bool,
    dataset_absolute_path: str,
    parameters_absolute_path: str,
    simulation_id: str,
    adx_uri: str,
    adx_ingest_uri: str,
    database_name: str,
    wait: bool,
):
    # Import the function at the start of the command
    from cosmotech.coal.azure.adx.scenario import send_scenario_data

    # Send scenario data to ADX
    send_scenario_data(
        dataset_absolute_path=dataset_absolute_path,
        parameters_absolute_path=parameters_absolute_path,
        simulation_id=simulation_id,
        adx_uri=adx_uri,
        adx_ingest_uri=adx_ingest_uri,
        database_name=database_name,
        send_parameters=send_parameters,
        send_datasets=send_datasets,
        wait=wait,
    )


if __name__ == "__main__":
    adx_send_scenariodata()
