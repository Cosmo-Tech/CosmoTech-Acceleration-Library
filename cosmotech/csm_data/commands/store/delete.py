# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import translate_help, web_help


@click.command()
@web_help("csm-data/store/delete")
@translate_help("csm_data.commands.store.delete.description")
def delete():
    # Import the function at the start of the command
    from cosmotech.coal.store.output import channel_spliter
    from cosmotech.coal.utils.configuration import Configuration

    try:
        _cs = channel_spliter.ChannelSpliter(Configuration())
        _cs.delete()
    except ValueError as e:
        raise click.Abort() from e
