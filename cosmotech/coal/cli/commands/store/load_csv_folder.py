# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.store.csv import store_csv_file
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@web_help("csm-data/store/load-csv-folder")
@click.option("--store-folder",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              help="The folder containing the store files",
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--csv-folder",
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              help="The folder containing the csv files to store",
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
def load_csv_folder(store_folder, csv_folder):
    """Running this command will find all csvs in the given folder and put them in the store"""
    for csv_path in pathlib.Path(csv_folder).glob("*.csv"):
        LOGGER.info(f"Found {csv_path.name}, storing it")
        store_csv_file(csv_path.name[:-4], csv_path, store=Store(False, store_folder))
