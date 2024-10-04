# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.coal.cli.commands.api.rds_send_store import rds_send_store
from cosmotech.coal.cli.commands.store.dump_to_postgresql import dump_to_postgresql
from cosmotech.coal.cli.commands.store.dump_to_s3 import dump_to_s3
from cosmotech.coal.cli.commands.store.list_tables import list_tables
from cosmotech.coal.cli.commands.store.load_csv_folder import load_csv_folder
from cosmotech.coal.cli.commands.store.reset import reset
from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help


@click.group()
@web_help("csm-data/store")
def store():
    """CoAL Data Store command group

This group of commands will give you helper commands to interact with the datastore
    """
    pass


store.add_command(rds_send_store, "rds-send-store")
store.add_command(reset, "reset")
store.add_command(list_tables, "list-tables")
store.add_command(load_csv_folder, "load-csv-folder")
store.add_command(dump_to_postgresql, "dump-to-postgresql")
store.add_command(dump_to_s3, "dump-to-s3")
