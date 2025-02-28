# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import web_help, translate_help
from cosmotech.orchestrator.utils.translate import T

VALID_TYPES = (
    "sqlite",
    "csv",
    "parquet",
)


@click.command()
@click.option(
    "--store-folder",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    help=T("csm-data.commands.store.dump_to_azure.parameters.store_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--output-type",
    default="sqlite",
    help=T("csm-data.commands.store.dump_to_azure.parameters.output_type"),
    type=click.Choice(VALID_TYPES, case_sensitive=False),
)
@click.option(
    "--account-name",
    "account_name",
    envvar="AZURE_ACCOUNT_NAME",
    help=T("csm-data.commands.store.dump_to_azure.parameters.account_name"),
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--container-name",
    "container_name",
    envvar="AZURE_CONTAINER_NAME",
    help=T("csm-data.commands.store.dump_to_azure.parameters.container_name"),
    type=str,
    show_envvar=True,
    default="",
)
@click.option(
    "--prefix",
    "file_prefix",
    envvar="CSM_DATA_PREFIX",
    help=T("csm-data.commands.store.dump_to_azure.parameters.prefix"),
    metavar="PREFIX",
    type=str,
    show_envvar=True,
    default="",
)
@click.option(
    "--tenant-id",
    "tenant_id",
    help=T("csm-data.commands.store.dump_to_azure.parameters.tenant_id"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AZURE_TENANT_ID",
)
@click.option(
    "--client-id",
    "client_id",
    help=T("csm-data.commands.store.dump_to_azure.parameters.client_id"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AZURE_CLIENT_ID",
)
@click.option(
    "--client-secret",
    "client_secret",
    help=T("csm-data.commands.store.dump_to_azure.parameters.client_secret"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AZURE_CLIENT_SECRET",
)
@web_help("csm-data/store/dump-to-azure")
@translate_help("csm-data.commands.store.dump_to_azure.description")
def dump_to_azure(
    store_folder,
    account_name: str,
    container_name: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    output_type: str,
    file_prefix: str,
):
    # Import the function at the start of the command
    from cosmotech.coal.azure import dump_store_to_azure

    try:
        dump_store_to_azure(
            store_folder=store_folder,
            account_name=account_name,
            container_name=container_name,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            output_type=output_type,
            file_prefix=file_prefix,
        )
    except ValueError as e:
        raise click.Abort() from e
