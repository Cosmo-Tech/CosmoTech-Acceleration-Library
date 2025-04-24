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
@web_help("csm-data/store/load-csv-folder")
@translate_help("csm-data.commands.store.load_csv_folder.description")
@click.option(
    "--store-folder",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    help=T("csm-data.commands.store.load_csv_folder.parameters.store_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--csv-folder",
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    help=T("csm-data.commands.store.load_csv_folder.parameters.csv_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
def load_csv_folder(store_folder, csv_folder):
    # Import the modules and functions at the start of the command
    import pathlib
    from cosmotech.coal.store.csv import store_csv_file
    from cosmotech.coal.store.store import Store
    from cosmotech.coal.utils.logger import LOGGER

    for csv_path in pathlib.Path(csv_folder).glob("*.csv"):
        LOGGER.info(T("coal.services.azure_storage.found_file").format(file=csv_path.name))
        store_csv_file(csv_path.name[:-4], csv_path, store=Store(False, store_folder))
