# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import sys
import warnings
from unittest.mock import patch

import pytest


class TestRunnerImports:
    """Tests for the runner module imports."""

    def test_module_imports(self):
        """Test that the module correctly imports and re-exports functions."""
        # Remove the module from sys.modules if it's already imported
        if "cosmotech.coal.cosmotech_api.runner" in sys.modules:
            del sys.modules["cosmotech.coal.cosmotech_api.runner"]

        # Import the module directly
        from cosmotech.coal.cosmotech_api.runner import get_runner_metadata

        # Verify that the function is imported correctly
        assert callable(get_runner_metadata)
