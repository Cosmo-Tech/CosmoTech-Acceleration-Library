# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import web_help, translate_help
from cosmotech.orchestrator.utils.translate import T


@click.command()
@web_help("csm-data/store/dump-to-mongodb")
@translate_help("csm_data.commands.store.dump_to_mongodb.description")
@click.option(
    "--store-folder",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    help=T("csm_data.commands.store.dump_to_mongodb.parameters.store_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--collection-prefix",
    help=T("csm_data.commands.store.dump_to_mongodb.parameters.collection_prefix"),
    metavar="PREFIX",
    type=str,
    default="Cosmotech_",
)
@click.option(
    "--mongodb-uri",
    help=T("csm_data.commands.store.dump_to_mongodb.parameters.mongodb_uri"),
    envvar="MONGODB_URI",
    show_envvar=True,
    required=True,
)
@click.option(
    "--mongodb-db",
    help=T("csm_data.commands.store.dump_to_mongodb.parameters.mongodb_db"),
    envvar="MONGODB_DB_NAME",
    show_envvar=True,
    required=True,
)
@click.option(
    "--replace/--append",
    "replace",
    help=T("csm_data.commands.store.dump_to_mongodb.parameters.replace"),
    default=True,
    is_flag=True,
    show_default=True,
)
def dump_to_mongodb(
    store_folder,
    collection_prefix: str,
    mongodb_uri,
    mongodb_db,
    replace: bool,
):
    # Import the function at the start of the command
    from cosmotech.coal.mongodb import dump_store_to_mongodb

    dump_store_to_mongodb(
        store_folder=store_folder,
        collection_prefix=collection_prefix,
        mongodb_uri=mongodb_uri,
        mongodb_db=mongodb_db,
        replace=replace,
    )
