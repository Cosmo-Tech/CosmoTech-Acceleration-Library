# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use reproduction translation broadcasting transmission distribution
# etc. to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest

from cosmotech.coal.utils import WEB_DOCUMENTATION_ROOT, strtobool


class TestUtilsInit:
    """Tests for the utils module initialization."""

    def test_web_documentation_root(self):
        """Test that WEB_DOCUMENTATION_ROOT is correctly defined."""
        # Verify that WEB_DOCUMENTATION_ROOT is a string
        assert isinstance(WEB_DOCUMENTATION_ROOT, str)

        # Verify that WEB_DOCUMENTATION_ROOT contains the expected URL pattern
        assert "https://cosmo-tech.github.io/CosmoTech-Acceleration-Library/" in WEB_DOCUMENTATION_ROOT

    def test_strtobool_true_values(self):
        """Test that strtobool correctly identifies true values."""
        true_values = ["y", "yes", "t", "true", "on", "1", "Y", "YES", "T", "TRUE", "ON", "1"]

        for value in true_values:
            assert strtobool(value) is True

    def test_strtobool_false_values(self):
        """Test that strtobool correctly identifies false values."""
        false_values = ["n", "no", "f", "false", "off", "0", "N", "NO", "F", "FALSE", "OFF", "0"]

        for value in false_values:
            assert strtobool(value) is False

    def test_strtobool_invalid_values(self):
        """Test that strtobool raises ValueError for invalid values."""
        invalid_values = ["", "maybe", "2", "truee", "falsee", "yess", "noo"]

        for value in invalid_values:
            with pytest.raises(ValueError) as excinfo:
                strtobool(value)

            # Verify that the error message contains the invalid value
            assert value in str(excinfo.value)
            assert "is not a recognized truth value" in str(excinfo.value)

    def test_strtobool_mixed_case(self):
        """Test that strtobool handles mixed case values correctly."""
        true_mixed_case = ["Yes", "TRUE", "On", "tRuE", "yEs"]
        false_mixed_case = ["No", "FALSE", "Off", "fAlSe", "nO"]

        for value in true_mixed_case:
            assert strtobool(value) is True

        for value in false_mixed_case:
            assert strtobool(value) is False
