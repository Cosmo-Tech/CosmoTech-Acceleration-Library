# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
from unittest.mock import MagicMock, patch

from cosmotech.coal.cosmotech_api.parameters import write_parameters


class TestParametersFunctions:
    """Tests for top-level functions in the parameters module."""

    @patch("cosmotech.coal.cosmotech_api.parameters.open")
    @patch("cosmotech.coal.cosmotech_api.parameters.DictWriter")
    @patch("cosmotech.coal.cosmotech_api.parameters.json.dump")
    @patch("cosmotech.coal.cosmotech_api.parameters.os.path.join")
    @patch("cosmotech.coal.cosmotech_api.parameters.LOGGER")
    def test_write_parameters_no_output(self, mock_logger, mock_join, mock_json_dump, mock_dict_writer, mock_open):
        """Test the write_parameters function with no output."""
        # Arrange
        parameter_folder = "/path/to/parameters"
        parameters = [
            {"parameterId": "param1", "value": "value1", "varType": "string", "isInherited": False},
        ]

        # Act
        write_parameters(parameter_folder, parameters, write_csv=False, write_json=False)

        # Assert
        # Check that no files were created
        mock_join.assert_not_called()
        mock_open.assert_not_called()
        mock_dict_writer.assert_not_called()
        mock_json_dump.assert_not_called()
        mock_logger.info.assert_not_called()
