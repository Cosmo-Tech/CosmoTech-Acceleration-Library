# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import click_log

from cosmotech.coal import __version__
from cosmotech.csm_data.commands.adx_send_runnerdata import adx_send_runnerdata
from cosmotech.csm_data.commands.api.api import api
from cosmotech.csm_data.commands.az_storage_upload import az_storage_upload
from cosmotech.csm_data.commands.s3_bucket_download import s3_bucket_download
from cosmotech.csm_data.commands.s3_bucket_upload import s3_bucket_upload
from cosmotech.csm_data.commands.s3_bucket_delete import s3_bucket_delete
from cosmotech.csm_data.commands.adx_send_data import adx_send_data
from cosmotech.csm_data.commands.store.store import store
from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import translate_help, web_help
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(f"Cosmo Tech Data Interface {__version__}")
    ctx.exit()


@click.group("csm-data")
@click_log.simple_verbosity_option(LOGGER, "--log-level", envvar="LOG_LEVEL", show_envvar=True)
@click.option(
    "--version",
    is_flag=True,
    callback=print_version,
    expose_value=False,
    is_eager=True,
    help="Print version number and return.",
)
@web_help("csm-data")
@translate_help("csm-data.commands.main.description")
def main():
    pass


main.add_command(api, "api")
main.add_command(store, "store")
main.add_command(s3_bucket_download, "s3-bucket-download")
main.add_command(s3_bucket_upload, "s3-bucket-upload")
main.add_command(s3_bucket_delete, "s3-bucket-delete")
main.add_command(adx_send_runnerdata, "adx-send-runnerdata")
main.add_command(az_storage_upload, "az-storage-upload")
main.add_command(adx_send_data, "adx-send-data")

if __name__ == "__main__":
    main()
