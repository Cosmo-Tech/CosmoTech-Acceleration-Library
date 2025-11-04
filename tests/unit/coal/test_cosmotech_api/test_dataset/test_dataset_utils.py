# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.


import pytest

from cosmotech.coal.cosmotech_api.dataset.utils import sheet_to_header


class TestUtilsFunctions:
    """Tests for top-level functions in the utils module."""

    def test_sheet_to_header_with_id(self):
        """Test the sheet_to_header function with id field."""
        # Arrange
        sheet_content = [
            {"id": "1", "name": "Alice", "age": 30},
            {"id": "2", "name": "Bob", "age": 25, "city": "New York"},
        ]

        # Act
        result = sheet_to_header(sheet_content)

        # Assert
        assert result[0] == "id"  # id should be first
        assert "name" in result
        assert "age" in result
        assert "city" in result
        assert len(result) == 4

    def test_sheet_to_header_with_source_target(self):
        """Test the sheet_to_header function with source and target fields."""
        # Arrange
        sheet_content = [
            {"source": "1", "target": "2", "weight": 10},
            {"source": "2", "target": "3", "weight": 20, "since": "2020"},
        ]

        # Act
        result = sheet_to_header(sheet_content)

        # Assert
        assert result[0] == "source"  # source should be first
        assert result[1] == "target"  # target should be second
        assert "weight" in result
        assert "since" in result
        assert len(result) == 4

    def test_sheet_to_header_with_id_and_source_target(self):
        """Test the sheet_to_header function with id, source, and target fields."""
        # Arrange
        sheet_content = [
            {"id": "100", "source": "1", "target": "2", "weight": 10},
            {"id": "101", "source": "2", "target": "3", "weight": 20},
        ]

        # Act
        result = sheet_to_header(sheet_content)

        # Assert
        assert result[0] == "id"  # id should be first
        assert result[1] == "source"  # source should be second
        assert result[2] == "target"  # target should be third
        assert "weight" in result
        assert len(result) == 4

    def test_sheet_to_header_empty(self):
        """Test the sheet_to_header function with empty data."""
        # Arrange
        sheet_content = []

        # Act
        result = sheet_to_header(sheet_content)

        # Assert
        assert result == []
