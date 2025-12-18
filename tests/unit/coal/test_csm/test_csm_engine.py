# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use reproduction translation broadcasting transmission distribution
# etc. to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import os
from unittest.mock import MagicMock, mock_open, patch

import pytest

from cosmotech.coal.csm.engine import apply_simple_csv_parameter_to_simulator


class TestCsmEngine:
    """Tests for the CSM Engine module."""

    @patch("os.path.exists")
    @patch("glob.glob")
    @patch("builtins.open", new_callable=mock_open, read_data='id,value\nentity1,"42"\nentity2,"true"\n')
    def test_apply_simple_csv_parameter_to_simulator(self, mock_file, mock_glob, mock_exists):
        """Test the apply_simple_csv_parameter_to_simulator function."""
        # Arrange
        mock_exists.return_value = True
        mock_glob.return_value = ["/path/to/parameter/file.csv"]

        # Create a mock simulator
        mock_simulator = MagicMock()
        mock_model = MagicMock()
        mock_simulator.GetModel.return_value = mock_model

        # Create mock entities
        mock_entity1 = MagicMock()
        mock_entity2 = MagicMock()

        # Configure model to return entities
        def find_entity_by_name(name):
            if name == "entity1":
                return mock_entity1
            elif name == "entity2":
                return mock_entity2
            else:
                return None

        mock_model.FindEntityByName.side_effect = find_entity_by_name

        # Set environment variable
        with patch.dict(os.environ, {"CSM_PARAMETERS_ABSOLUTE_PATH": "/path/to/parameter/"}):
            # Act
            apply_simple_csv_parameter_to_simulator(
                simulator=mock_simulator,
                parameter_name="test_parameter",
                target_attribute_name="test_attribute",
                csv_id_column="id",
                csv_value_column="value",
            )

            # Assert
            mock_exists.assert_called_once_with("/path/to/parameter/test_parameter")
            mock_glob.assert_called_once_with("/path/to/parameter/test_parameter/*.csv")
            mock_file.assert_called_once_with("/path/to/parameter/file.csv", "r")

            # Check that the model was retrieved
            mock_simulator.GetModel.assert_called_once()

            # Check that FindEntityByName was called for each row
            assert mock_model.FindEntityByName.call_count == 2
            mock_model.FindEntityByName.assert_any_call("entity1")
            mock_model.FindEntityByName.assert_any_call("entity2")

            # Check that SetAttributeAsString was called for each entity
            mock_entity1.SetAttributeAsString.assert_called_once_with("test_attribute", json.dumps(42))
            mock_entity2.SetAttributeAsString.assert_called_once_with("test_attribute", json.dumps(True))

    @patch("os.path.exists")
    def test_apply_simple_csv_parameter_to_simulator_parameter_not_exists(self, mock_exists):
        """Test the apply_simple_csv_parameter_to_simulator function when parameter does not exist."""
        # Arrange
        mock_exists.return_value = False
        mock_simulator = MagicMock()

        # Set environment variable
        with patch.dict(os.environ, {"CSM_PARAMETERS_ABSOLUTE_PATH": "/path/to/parameter/"}):
            # Act & Assert
            with pytest.raises(ValueError, match="Parameter test_parameter does not exists."):
                apply_simple_csv_parameter_to_simulator(
                    simulator=mock_simulator, parameter_name="test_parameter", target_attribute_name="test_attribute"
                )

            # Assert
            mock_exists.assert_called_once_with("/path/to/parameter/test_parameter")
            mock_simulator.GetModel.assert_not_called()

    @patch("os.path.exists")
    @patch("glob.glob")
    def test_apply_simple_csv_parameter_to_simulator_no_csv_files(self, mock_glob, mock_exists):
        """Test the apply_simple_csv_parameter_to_simulator function when no CSV files are found."""
        # Arrange
        mock_exists.return_value = True
        mock_glob.return_value = []
        mock_simulator = MagicMock()

        # Set environment variable
        with patch.dict(os.environ, {"CSM_PARAMETERS_ABSOLUTE_PATH": "/path/to/parameter/"}):
            # Act
            apply_simple_csv_parameter_to_simulator(
                simulator=mock_simulator, parameter_name="test_parameter", target_attribute_name="test_attribute"
            )

            # Assert
            mock_exists.assert_called_once_with("/path/to/parameter/test_parameter")
            mock_glob.assert_called_once_with("/path/to/parameter/test_parameter/*.csv")
            mock_simulator.GetModel.assert_not_called()

    @patch("os.path.exists")
    @patch("glob.glob")
    @patch("builtins.open", new_callable=mock_open, read_data='id,value\nentity1,"42"\nentity_not_found,"true"\n')
    def test_apply_simple_csv_parameter_to_simulator_entity_not_found(self, mock_file, mock_glob, mock_exists):
        """Test the apply_simple_csv_parameter_to_simulator function when an entity is not found."""
        # Arrange
        mock_exists.return_value = True
        mock_glob.return_value = ["/path/to/parameter/file.csv"]

        # Create a mock simulator
        mock_simulator = MagicMock()
        mock_model = MagicMock()
        mock_simulator.GetModel.return_value = mock_model

        # Create mock entity
        mock_entity1 = MagicMock()

        # Configure model to return entities
        def find_entity_by_name(name):
            if name == "entity1":
                return mock_entity1
            else:
                return None

        mock_model.FindEntityByName.side_effect = find_entity_by_name

        # Set environment variable
        with patch.dict(os.environ, {"CSM_PARAMETERS_ABSOLUTE_PATH": "/path/to/parameter/"}):
            # Act
            apply_simple_csv_parameter_to_simulator(
                simulator=mock_simulator, parameter_name="test_parameter", target_attribute_name="test_attribute"
            )

            # Assert
            mock_exists.assert_called_once_with("/path/to/parameter/test_parameter")
            mock_glob.assert_called_once_with("/path/to/parameter/test_parameter/*.csv")
            mock_file.assert_called_once_with("/path/to/parameter/file.csv", "r")

            # Check that the model was retrieved
            mock_simulator.GetModel.assert_called_once()

            # Check that FindEntityByName was called for each row
            assert mock_model.FindEntityByName.call_count == 2
            mock_model.FindEntityByName.assert_any_call("entity1")
            mock_model.FindEntityByName.assert_any_call("entity_not_found")

            # Check that SetAttributeAsString was called only for the found entity
            mock_entity1.SetAttributeAsString.assert_called_once_with("test_attribute", json.dumps(42))
