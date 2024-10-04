# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import click_log

from CosmoTech_Acceleration_Library import __version__
from cosmotech.coal.cli.commands.adx_send_scenariodata import adx_send_scenariodata
from cosmotech.coal.cli.commands.api.api import api
from cosmotech.coal.cli.commands.legacy.legacy import legacy
from cosmotech.coal.cli.commands.s3_bucket_download import s3_bucket_download
from cosmotech.coal.cli.commands.s3_bucket_upload import s3_bucket_upload
from cosmotech.coal.cli.commands.store.store import store
from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.utils.logger import LOGGER


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(f"Cosmo Tech Data Interface {__version__}")
    ctx.exit()
    

@click.group("csm-data")
@click_log.simple_verbosity_option(LOGGER,
                                   "--log-level",
                                   envvar="LOG_LEVEL",
                                   show_envvar=True)
@click.option('--version',
              is_flag=True,
              callback=print_version,
              expose_value=False,
              is_eager=True,
              help="Print version number and return.")
@web_help("csm-data")
def main():
    """Cosmo Tect Data Interface

Command toolkit provinding quick implementation of data connections to use inside the Cosmo Tech Platform"""
    pass


main.add_command(api, "api")
main.add_command(legacy, "legacy")
main.add_command(store, "store")
main.add_command(s3_bucket_download, "s3-bucket-download")
main.add_command(s3_bucket_upload, "s3-bucket-upload")
main.add_command(adx_send_scenariodata, "adx-send-scenariodata")

if __name__ == "__main__":
    main()
