# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from cosmotech.orchestrator.utils.translate import T

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import web_help, translate_help


@click.command()
@web_help("csm-data/adx-send-data")
@translate_help("csm-data.commands.storage.adx_send_data.description")
@click.option(
    "--adx-uri",
    envvar="AZURE_DATA_EXPLORER_RESOURCE_URI",
    show_envvar=True,
    required=True,
    metavar="URI",
    help=T("csm-data.commands.storage.adx_send_data.parameters.adx_uri"),
)
@click.option(
    "--adx-ingest-uri",
    envvar="AZURE_DATA_EXPLORER_RESOURCE_INGEST_URI",
    show_envvar=True,
    required=True,
    metavar="URI",
    help=T("csm-data.commands.storage.adx_send_data.parameters.adx_ingest_uri"),
)
@click.option(
    "--database-name",
    envvar="AZURE_DATA_EXPLORER_DATABASE_NAME",
    show_envvar=True,
    required=True,
    metavar="NAME",
    help=T("csm-data.commands.storage.adx_send_data.parameters.database_name"),
)
@click.option(
    "--wait/--no-wait",
    "wait",
    envvar="CSM_DATA_ADX_WAIT_INGESTION",
    show_envvar=True,
    default=False,
    show_default=True,
    help=T("csm-data.commands.storage.adx_send_data.parameters.waiting_ingestion"),
)
@click.option(
    "--tag",
    envvar="CSM_DATA_ADX_TAG",
    show_envvar=True,
    default=None,
    help=T("csm-data.commands.storage.adx_send_data.parameters.adx_tag"),
)
@click.option(
    "--store-folder",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    help=T("csm-data.commands.storage.adx_send_data.parameters.store_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
def adx_send_data(
    adx_uri: str,
    adx_ingest_uri: str,
    database_name: str,
    wait: bool,
    store_folder: str,
    tag: str = None,
):
    """
    Send data from the store to Azure Data Explorer.
    """
    from cosmotech.coal.azure.adx.store import send_store_to_adx

    success = send_store_to_adx(
        adx_uri=adx_uri,
        adx_ingest_uri=adx_ingest_uri,
        database_name=database_name,
        wait=wait,
        tag=tag,
        store_location=store_folder,
    )

    if not success:
        click.Abort()


if __name__ == "__main__":
    adx_send_data()
