# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from cosmotech.coal.azure.adx.utils import type_mapping


class TestUtilsFunctions:
    """Tests for top-level functions in the utils module."""

    def test_type_mapping_simulation_run(self):
        """Test the type_mapping function with SimulationRun key."""
        # Arrange
        key = "SimulationRun"
        value = "any-value"

        # Act
        result = type_mapping(key, value)

        # Assert
        assert result == "guid"

    def test_type_mapping_datetime_string(self):
        """Test the type_mapping function with a datetime string."""
        # Arrange
        key = "date"
        value = "2023-01-01T12:00:00Z"

        # Act
        result = type_mapping(key, value)

        # Assert
        assert result == "datetime"

    def test_type_mapping_float(self):
        """Test the type_mapping function with a float value."""
        # Arrange
        key = "temperature"
        value = 22.5

        # Act
        result = type_mapping(key, value)

        # Assert
        assert result == "real"

    def test_type_mapping_int(self):
        """Test the type_mapping function with an integer value."""
        # Arrange
        key = "count"
        value = 42

        # Act
        result = type_mapping(key, value)

        # Assert
        assert result == "long"

    def test_type_mapping_string(self):
        """Test the type_mapping function with a string value."""
        # Arrange
        key = "name"
        value = "test-name"

        # Act
        result = type_mapping(key, value)

        # Assert
        assert result == "string"

    def test_type_mapping_boolean(self):
        """Test the type_mapping function with a boolean value."""
        # Arrange
        key = "active"
        value = True

        # Act
        result = type_mapping(key, value)

        # Assert
        assert result == "long"  # Booleans are treated as integers (long) in the implementation

    def test_type_mapping_none(self):
        """Test the type_mapping function with a None value."""
        # Arrange
        key = "nullable"
        value = None

        # Act
        result = type_mapping(key, value)

        # Assert
        assert result == "string"

    def test_type_mapping_invalid_datetime(self):
        """Test the type_mapping function with an invalid datetime string."""
        # Arrange
        key = "not_a_date"
        value = "not-a-date-string"

        # Act
        result = type_mapping(key, value)

        # Assert
        assert result == "string"
