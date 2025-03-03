# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use reproduction translation broadcasting transmission distribution
# etc. to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import importlib
import sys
import warnings
from unittest.mock import patch, MagicMock

import pytest


class TestCosmoTechApiRunner:
    """Tests for the runner compatibility module."""

    def test_module_content(self):
        """Test that the module has the expected content and exports."""
        # Define mock for the imported function
        mock_get_runner_metadata = MagicMock()

        # Create patch for the imported function
        with patch("cosmotech.coal.cosmotech_api.runner.metadata.get_runner_metadata", mock_get_runner_metadata):
            # Remove the module from sys.modules if it's already imported
            if "cosmotech.coal.cosmotech_api.runner" in sys.modules:
                del sys.modules["cosmotech.coal.cosmotech_api.runner"]

            # Import the module (this will use our mocked function)
            import cosmotech.coal.cosmotech_api.runner

            # Verify that the module has the expected attribute
            assert hasattr(cosmotech.coal.cosmotech_api.runner, "get_runner_metadata")

            # Verify that the attribute is our mocked function
            assert cosmotech.coal.cosmotech_api.runner.get_runner_metadata is mock_get_runner_metadata

            # Verify that the module has the expected docstring
            assert "Runner" in cosmotech.coal.cosmotech_api.runner.__doc__
