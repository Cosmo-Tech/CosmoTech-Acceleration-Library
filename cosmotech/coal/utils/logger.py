# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import logging
import os

from rich.logging import RichHandler

LOGGER = logging.getLogger("csm.data")

_format = "%(message)s"

if "PAILLETTES" in os.environ:
    paillettes = "[bold yellow blink]***[/]"
    _format = f"{paillettes} {_format} {paillettes}"

logging.basicConfig(
    format=_format,
    datefmt="[%Y/%m/%d-%X]",
    handlers=[RichHandler(rich_tracebacks=True,
                          omit_repeated_times=False,
                          show_path=False,
                          markup=True)])
LOGGER.setLevel(logging.INFO)
