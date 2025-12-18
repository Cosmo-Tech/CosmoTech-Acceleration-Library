# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import patch

import pytest

from cosmotech.coal.store.output.postgres_channel import PostgresChannel
from cosmotech.coal.utils.configuration import Configuration


@pytest.fixture
def base_postgres_config():
    return Configuration(
        {
            "cosmotech": {
                "parameters_absolute_path": "/path/to/dataset",
                "organization_id": "org123",
                "workspace_id": "ws456",
                "runner_id": "run789",
            },
            "postgres": {
                "host": "localhost",
                "port": "5432",
                "db_name": "testdb",
                "db_schema": "public",
                "user_name": "testuser",
                "user_password": "testpass",
            },
        }
    )


class TestPostgresChannel:
    """Tests for the PostgresChannel class."""

    def test_init_with_configuration(self, base_postgres_config):
        """Test PostgresChannel initialization with configuration."""
        # Act
        channel = PostgresChannel(base_postgres_config)

        # Assert
        assert channel.configuration is not None

    def test_required_keys(self):
        """Test that required_keys are properly defined."""
        # Assert
        assert "coal" in PostgresChannel.required_keys
        assert "store" in PostgresChannel.required_keys["coal"]
        assert "cosmotech" in PostgresChannel.required_keys
        assert "organization_id" in PostgresChannel.required_keys["cosmotech"]
        assert "workspace_id" in PostgresChannel.required_keys["cosmotech"]
        assert "runner_id" in PostgresChannel.required_keys["cosmotech"]
        assert "postgres" in PostgresChannel.required_keys
        assert "host" in PostgresChannel.required_keys["postgres"]
        assert "port" in PostgresChannel.required_keys["postgres"]
        assert "db_name" in PostgresChannel.required_keys["postgres"]
        assert "db_schema" in PostgresChannel.required_keys["postgres"]
        assert "user_name" in PostgresChannel.required_keys["postgres"]
        assert "user_password" in PostgresChannel.required_keys["postgres"]

    @patch("cosmotech.coal.store.output.postgres_channel.dump_store_to_postgresql_from_conf")
    @patch("cosmotech.coal.store.output.postgres_channel.send_runner_metadata_to_postgresql")
    def test_send_without_filter(self, mock_send_metadata, mock_dump, base_postgres_config):
        """Test sending data without table filter."""
        mock_send_metadata.return_value = "run_id_123"
        channel = PostgresChannel(base_postgres_config)

        # Act
        channel.send()

        # Assert
        mock_send_metadata.assert_called_once()
        mock_dump.assert_called_once()

        # Check the arguments passed to dump_store_to_postgresql_from_conf
        call_args = mock_dump.call_args
        assert call_args.kwargs["configuration"] == base_postgres_config
        assert call_args.kwargs["selected_tables"] is None
        assert call_args.kwargs["fk_id"] == "run_id_123"

    @patch("cosmotech.coal.store.output.postgres_channel.dump_store_to_postgresql_from_conf")
    @patch("cosmotech.coal.store.output.postgres_channel.send_runner_metadata_to_postgresql")
    def test_send_with_filter(self, mock_send_metadata, mock_dump, base_postgres_config):
        """Test sending data with table filter."""
        mock_send_metadata.return_value = "run_id_456"
        channel = PostgresChannel(base_postgres_config)
        tables_filter = ["table1", "table2", "table3"]

        # Act
        channel.send(filter=tables_filter)

        # Assert
        mock_send_metadata.assert_called_once()
        mock_dump.assert_called_once()

        # Check the arguments passed to dump_store_to_postgresql_from_conf
        call_args = mock_dump.call_args
        assert call_args[1]["configuration"] == base_postgres_config
        assert call_args[1]["selected_tables"] == ["table1", "table2", "table3"]
        assert call_args[1]["fk_id"] == "run_id_456"

    @patch("cosmotech.coal.store.output.postgres_channel.remove_runner_metadata_from_postgresql")
    def test_delete(self, mock_remove_metadata, base_postgres_config):
        """Test delete method."""

        channel = PostgresChannel(base_postgres_config)

        # Act
        channel.delete()

        # Assert
        mock_remove_metadata.assert_called_once()
        # Check that configuration was passed
        call_args = mock_remove_metadata.call_args
        assert call_args.args[0] == base_postgres_config
