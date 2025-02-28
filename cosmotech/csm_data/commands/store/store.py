# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.csm_data.commands.api.rds_send_store import rds_send_store
from cosmotech.csm_data.commands.store.dump_to_azure import dump_to_azure
from cosmotech.csm_data.commands.store.dump_to_postgresql import dump_to_postgresql
from cosmotech.csm_data.commands.store.dump_to_s3 import dump_to_s3
from cosmotech.csm_data.commands.store.list_tables import list_tables
from cosmotech.csm_data.commands.store.load_csv_folder import load_csv_folder
from cosmotech.csm_data.commands.store.load_from_singlestore import (
    load_from_singlestore_command,
)
from cosmotech.csm_data.commands.store.reset import reset
from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import web_help, translate_help
from cosmotech.orchestrator.utils.translate import T


@click.group()
@web_help("csm-data/store")
@translate_help("csm-data.commands.store.description")
def store():
    pass


store.add_command(rds_send_store, "rds-send-store")
store.add_command(reset, "reset")
store.add_command(list_tables, "list-tables")
store.add_command(load_csv_folder, "load-csv-folder")
store.add_command(load_from_singlestore_command, "load-from-singlestore")
store.add_command(dump_to_postgresql, "dump-to-postgresql")
store.add_command(dump_to_s3, "dump-to-s3")
store.add_command(dump_to_azure, "dump-to-azure")
