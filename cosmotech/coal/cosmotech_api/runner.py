# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Runner module (compatibility layer).

This module is maintained for backward compatibility.
The functionality has been moved to the cosmotech.coal.cosmotech_api.runner package.
"""

import warnings

warnings.warn(
    "Direct imports from cosmotech.coal.cosmotech_api.runner.py are deprecated. "
    "Please import from cosmotech.coal.cosmotech_api.runner package instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from the runner package
from cosmotech.coal.cosmotech_api.runner.metadata import get_runner_metadata
