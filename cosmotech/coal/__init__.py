# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import importlib.metadata
import pathlib
import tomllib


def get_project_version():
    """Get the project version, as specified in the pyproject.toml file"""
    # In editable mode the installed version may not reflect the current state
    # So check for a pyproject.toml from source as a first source
    pyproject_path = pathlib.Path(__file__).parent.parent.parent / "pyproject.toml"
    if pyproject_path.exists():
        with open(pyproject_path, "rb") as f:
            return tomllib.load(f)["project"]["version"]

    # Otherwise report the installed version
    return importlib.metadata.version("cosmotech_acceleration_library")


__version__ = get_project_version()
