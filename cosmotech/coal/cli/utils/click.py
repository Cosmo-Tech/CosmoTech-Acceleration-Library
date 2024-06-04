# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import rich_click as click

click.rich_click.USE_MARKDOWN = True
click.rich_click.USE_RICH_MARKUP = True
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = False
click.rich_click.STYLE_OPTION_ENVVAR = "yellow"
click.rich_click.ENVVAR_STRING = "ENV: {}"
click.rich_click.STYLE_OPTION_DEFAULT = "dim yellow"
click.rich_click.DEFAULT_STRING = "DEFAULT: {}"
click.rich_click.OPTIONS_PANEL_TITLE = "OPTIONS"
