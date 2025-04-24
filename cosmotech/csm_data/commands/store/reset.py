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
@web_help("csm-data/store/reset")
@translate_help("csm-data.commands.store.reset.description")
@click.option(
    "--store-folder",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    help=T("csm-data.commands.store.reset.parameters.store_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
def reset(store_folder):
    # Import the modules and functions at the start of the command
    from cosmotech.coal.store.store import Store
    from cosmotech.coal.utils.logger import LOGGER

    Store(True, store_folder)
    LOGGER.info(T("coal.services.database.store_reset").format(folder=store_folder))
