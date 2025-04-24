# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
from unittest.mock import MagicMock, patch

from cosmotech.coal.cosmotech_api.dataset.utils import get_content_from_twin_graph_data, sheet_to_header


class TestUtilsFunctions:
    """Tests for top-level functions in the utils module."""

    def test_get_content_from_twin_graph_data_default(self):
        """Test the get_content_from_twin_graph_data function with default settings."""
        # Arrange
        nodes = [
            {
                "n": {
                    "id": "50",
                    "label": "Customer",
                    "properties": {"Satisfaction": 0, "Thirsty": False, "id": "Lars_Coret"},
                    "type": "NODE",
                }
            },
            {"n": {"id": "51", "label": "Shop", "properties": {"Open": True, "id": "Coffee_Shop"}, "type": "NODE"}},
        ]

        relationships = [
            {
                "src": {"id": "50", "label": "Customer", "properties": {"id": "Lars_Coret"}},
                "dest": {"id": "51", "label": "Shop", "properties": {"id": "Coffee_Shop"}},
                "rel": {"id": "100", "label": "VISITS", "properties": {"frequency": "daily"}},
            }
        ]

        # Act
        result = get_content_from_twin_graph_data(nodes, relationships)

        # Assert
        assert "Customer" in result
        assert "Shop" in result
        assert "VISITS" in result

        # Check node content
        assert len(result["Customer"]) == 1
        assert result["Customer"][0]["id"] == "50"  # Uses node ID
        assert result["Customer"][0]["Satisfaction"] == 0
        assert result["Customer"][0]["Thirsty"] is False

        assert len(result["Shop"]) == 1
        assert result["Shop"][0]["id"] == "51"  # Uses node ID
        assert result["Shop"][0]["Open"] is True

        # Check relationship content
        assert len(result["VISITS"]) == 1
        assert result["VISITS"][0]["id"] == "100"
        assert result["VISITS"][0]["source"] == "50"  # Uses node ID
        assert result["VISITS"][0]["target"] == "51"  # Uses node ID
        assert result["VISITS"][0]["frequency"] == "daily"

    def test_get_content_from_twin_graph_data_restore_names(self):
        """Test the get_content_from_twin_graph_data function with restore_names=True."""
        # Arrange
        nodes = [
            {
                "n": {
                    "id": "50",
                    "label": "Customer",
                    "properties": {"Satisfaction": 0, "Thirsty": False, "id": "Lars_Coret"},
                    "type": "NODE",
                }
            },
            {"n": {"id": "51", "label": "Shop", "properties": {"Open": True, "id": "Coffee_Shop"}, "type": "NODE"}},
        ]

        relationships = [
            {
                "src": {"id": "50", "label": "Customer", "properties": {"id": "Lars_Coret"}},
                "dest": {"id": "51", "label": "Shop", "properties": {"id": "Coffee_Shop"}},
                "rel": {"id": "100", "label": "VISITS", "properties": {"frequency": "daily"}},
            }
        ]

        # Act
        result = get_content_from_twin_graph_data(nodes, relationships, restore_names=True)

        # Assert
        assert "Customer" in result
        assert "Shop" in result
        assert "VISITS" in result

        # Check node content
        assert len(result["Customer"]) == 1
        assert result["Customer"][0]["id"] == "Lars_Coret"  # Uses property ID
        assert result["Customer"][0]["Satisfaction"] == 0
        assert result["Customer"][0]["Thirsty"] is False

        assert len(result["Shop"]) == 1
        assert result["Shop"][0]["id"] == "Coffee_Shop"  # Uses property ID
        assert result["Shop"][0]["Open"] is True

        # Check relationship content
        assert len(result["VISITS"]) == 1
        assert result["VISITS"][0]["id"] == "100"
        assert result["VISITS"][0]["source"] == "Lars_Coret"  # Uses property ID
        assert result["VISITS"][0]["target"] == "Coffee_Shop"  # Uses property ID
        assert result["VISITS"][0]["frequency"] == "daily"

    def test_get_content_from_twin_graph_data_empty(self):
        """Test the get_content_from_twin_graph_data function with empty data."""
        # Arrange
        nodes = []
        relationships = []

        # Act
        result = get_content_from_twin_graph_data(nodes, relationships)

        # Assert
        assert result == {}

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
