# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER


@click.group(invoke_without_command=True)
def api():
    """Cosmo Tech API helper command

This command will inform you of which connection is available to use for the Cosmo Tech API

If no connection is available, will list all possible set of parameters and return an error code,

You can use this command in a csm-orc template to make sure that API connection is available.
    """
    try:
        api_client, description = get_api_client()

        LOGGER.info(f"Found valid connection of type: {description}")
    except EnvironmentError:
        raise click.Abort()
