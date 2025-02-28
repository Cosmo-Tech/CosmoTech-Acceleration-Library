# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import pathlib
import time
import csv
import singlestoredb as s2
from sqlite3 import Cursor

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help, translate_help
from cosmotech.coal.store.csv import store_csv_file
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def get_data(table_name: str, output_directory: str, cursor: Cursor):
    """
    Run a SQL query to fetch all data from a table and write them in csv files
    """
    start_time = time.perf_counter()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    end_time = time.perf_counter()
    LOGGER.info(
        T("coal.logs.database.rows_fetched").format(
            table=table_name, count=len(rows), time=round(end_time - start_time, 2)
        )
    )
    with open(f"{output_directory}/{table_name}.csv", "w", newline="") as csv_stock:
        w = csv.DictWriter(csv_stock, rows[0].keys())
        w.writeheader()
        w.writerows(rows)


@click.command()
@web_help("csm-data/store/load-from-singlestore")
@translate_help("coal-help.commands.store.load_from_singlestore.description")
@click.option(
    "--singlestore-host",
    "single_store_host",
    envvar="SINGLE_STORE_HOST",
    help=T("coal-help.commands.store.load_from_singlestore.parameters.singlestore_host"),
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--singlestore-port",
    "single_store_port",
    help=T("coal-help.commands.store.load_from_singlestore.parameters.singlestore_port"),
    envvar="SINGLE_STORE_PORT",
    show_envvar=True,
    required=False,
    default=3306,
)
@click.option(
    "--singlestore-db",
    "single_store_db",
    help=T("coal-help.commands.store.load_from_singlestore.parameters.singlestore_db"),
    envvar="SINGLE_STORE_DB",
    show_envvar=True,
    required=True,
)
@click.option(
    "--singlestore-user",
    "single_store_user",
    help=T("coal-help.commands.store.load_from_singlestore.parameters.singlestore_user"),
    envvar="SINGLE_STORE_USERNAME",
    show_envvar=True,
    required=True,
)
@click.option(
    "--singlestore-password",
    "single_store_password",
    help=T("coal-help.commands.store.load_from_singlestore.parameters.singlestore_password"),
    envvar="SINGLE_STORE_PASSWORD",
    show_envvar=True,
    required=True,
)
@click.option(
    "--singlestore-tables",
    "single_store_tables",
    help=T("coal-help.commands.store.load_from_singlestore.parameters.singlestore_tables"),
    envvar="SINGLE_STORE_TABLES",
    show_envvar=True,
    required=True,
)
@click.option(
    "--store-folder",
    "store_folder",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    help=T("coal-help.commands.store.load_from_singlestore.parameters.store_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
def load_from_singlestore(
    single_store_host,
    single_store_port,
    single_store_db,
    single_store_user,
    single_store_password,
    store_folder,
    single_store_tables: str = "",
):
    single_store_working_dir = store_folder + "/singlestore"
    if not pathlib.Path.exists(single_store_working_dir):
        pathlib.Path.mkdir(single_store_working_dir)

    start_full = time.perf_counter()

    conn = s2.connect(
        host=single_store_host,
        port=single_store_port,
        database=single_store_db,
        user=single_store_user,
        password=single_store_password,
        results_type="dicts",
    )
    with conn:
        with conn.cursor() as cur:
            table_names = single_store_tables.split(",")
            if not table_names:
                cur.execute("SHOW TABLES")
                table_names = cur.fetchall()
            LOGGER.info(T("coal.logs.database.tables_to_fetch").format(tables=table_names))
            for name in table_names:
                get_data(name, single_store_working_dir, cur)
    end_full = time.perf_counter()
    LOGGER.info(T("coal.logs.database.full_dataset").format(time=round(end_full - start_full, 2)))

    for csv_path in pathlib.Path(single_store_working_dir).glob("*.csv"):
        LOGGER.info(T("coal.logs.storage.found_file").format(file=csv_path.name))
        store_csv_file(csv_path.name[:-4], csv_path, store=Store(False, store_folder))
