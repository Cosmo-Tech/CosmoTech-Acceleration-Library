# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import click_log

from CosmoTech_Acceleration_Library import __version__
from cosmotech.coal.cli.commands.rds_send_csv import rds_send_csv_command
from cosmotech.coal.cli.commands.s3_bucket_loader import s3_bucket_load_command
from cosmotech.coal.cli.utils.click import click
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
def main():
    """Cosmo Tect Data Interface

Command toolkit provinding quick implementation of data connections to use inside the Cosmo Tech Platform"""
    pass


main.add_command(s3_bucket_load_command, "s3-bucket-load")
main.add_command(rds_send_csv_command, "rds-send-csv")

if __name__ == "__main__":
    main()
