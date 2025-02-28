# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.coal import __version__

WEB_DOCUMENTATION_ROOT = f"https://cosmo-tech.github.io/CosmoTech-Acceleration-Library/{__version__}/"


def strtobool(string: str) -> bool:
    if string.lower() in ["y", "yes", "t", "true", "on", "1"]:
        return True
    if string.lower() in ["n", "no", "f", "false", "off", "0"]:
        return False
    raise ValueError(f'"{string} is not a recognized truth value')
