# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from cosmotech.orchestrator.utils.translate import T

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import translate_help, web_help


@click.command()
@web_help("csm-data/store/load-parquet-folder")
@translate_help("csm_data.commands.store.load_parquet_folder.description")
@click.option(
    "--store-folder",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    help=T("csm_data.commands.store.load_parquet_folder.parameters.store_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--parquet-folder",
    envvar="CSM_OUTPUT_ABSOLUTE_PATH",
    help=T("csm_data.commands.store.load_parquet_folder.parameters.parquet_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
def load_parquet_folder(store_folder, parquet_folder):
    # Import the modules and functions at the start of the command
    import pathlib

    from cosmotech.coal.store.parquet import store_parquet_file
    from cosmotech.coal.store.store import Store
    from cosmotech.coal.utils.configuration import Configuration
    from cosmotech.coal.utils.logger import LOGGER

    _conf = Configuration()
    _conf.coal.store = store_folder

    store = Store(False, _conf)
    for parquet_path in pathlib.Path(parquet_folder).glob("*.parquet"):
        LOGGER.info(T("coal.services.azure_storage.found_file").format(file=parquet_path.name))
        store_parquet_file(parquet_path.name[:-8], parquet_path, store=store)
