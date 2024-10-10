# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import logging
import os

from rich.logging import RichHandler
from rich.highlighter import NullHighlighter

HIGLIGHTER = NullHighlighter()
HANDLER = RichHandler(rich_tracebacks=True,
                      omit_repeated_times=False,
                      show_path=False,
                      markup=True,
                      highlighter=HIGLIGHTER)
_format = "%(message)s"

if "PAILLETTES" in os.environ:
    paillettes = "[bold yellow blink]***[/]"
    _format = f"{paillettes} {_format} {paillettes}"

FORMATTER = logging.Formatter(fmt=_format,
                              datefmt="[%Y/%m/%d-%H:%M:%S]",
                              )

HANDLER.setFormatter(FORMATTER)


def get_logger(
    logger_name: str,
    level=logging.INFO
) -> logging.Logger:
    _logger = logging.getLogger(logger_name)
    if not _logger.hasHandlers():
        _logger.addHandler(HANDLER)
    _logger.setLevel(level)
    return _logger


LOGGER = get_logger("csm.data")
