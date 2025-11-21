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
@click.option(
    "--filter",
    "-f",
    multiple=True,
    help=T("csm_data.commands.store.output.parameters.filter"),
    type=str,
)
@web_help("csm-data/store/output")
@translate_help("csm_data.commands.store.output.description")
def output(
    filter,
):
    # Import the function at the start of the command
    from cosmotech.coal.store.output import channel_spliter
    from cosmotech.coal.utils.configuration import Configuration

    try:
        _cs = channel_spliter.ChannelSpliter(Configuration())
        _cs.send(list(filter))
    except ValueError as e:
        raise click.Abort() from e
