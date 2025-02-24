# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help, translate_help
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


@click.command()
@web_help("csm-data/store/reset")
@translate_help("coal-help.commands.store.reset.description")
@click.option("--store-folder",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              help=T("coal-help.commands.store.reset.parameters.store_folder"),
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
def reset(store_folder):
    Store(True, store_folder)
    LOGGER.info(T("coal.logs.database.store_reset").format(folder=store_folder))
