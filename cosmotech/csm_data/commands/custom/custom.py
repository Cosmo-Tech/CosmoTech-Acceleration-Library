# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.csm_data.commands.custom.load_csv_file import load_csv_file
from cosmotech.csm_data.commands.custom.load_csv_folder import load_csv_folder
from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import translate_help, web_help


@click.group()
@web_help("csm-data/custom")
@translate_help("csm_data.commands.custom.custom.description")
def custom():
    pass


custom.add_command(load_csv_file, "load-csv-file")
custom.add_command(load_csv_folder, "load-csv-folder")
