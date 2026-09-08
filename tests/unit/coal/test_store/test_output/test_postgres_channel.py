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
                "runner_id": "r789",
                "run_id": "run789",
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

    def test_init(self, base_postgres_config):
        """Test PostgresChannel initialization with configuration."""
        # Act
        channel = PostgresChannel(base_postgres_config)

        # Assert default value for setup_db is added
        base_postgres_config.setup_db = True
        assert channel.configuration == base_postgres_config

    def test_init_with_setup_db(self, base_postgres_config):
        """Test PostgresChannel initialization with configuration."""
        # Act
        base_postgres_config.setup_db = False  # Set to False to test default behavior
        channel = PostgresChannel(base_postgres_config)

        # Assert default value for setup_db is added
        assert channel.configuration == base_postgres_config

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

    @patch("cosmotech.coal.store.output.postgres_channel.create_metadata")
    @patch("cosmotech.coal.store.output.postgres_channel.get_metadata_last_run")
    @patch("cosmotech.coal.store.output.postgres_channel.dump_store_to_postgresql_from_conf")
    @patch("cosmotech.coal.store.output.postgres_channel.send_runner_metadata_to_postgresql")
    @patch("cosmotech.coal.store.output.postgres_channel.add_fk_constraints")
    @patch("cosmotech.coal.store.output.postgres_channel.remove_run_metadata_from_postgresql")
    def test_send_no_setup_db(
        self,
        mock_remove_run,
        mock_add_fk,
        mock_send_metadata,
        mock_dump,
        mock_get_metadata_last_run,
        mock_create_metadata,
        base_postgres_config,
    ):
        """Test sending data without table filter."""

        base_postgres_config.setup_db = False  # Set to False to test behavior when setup_db is False
        channel = PostgresChannel(base_postgres_config)
        mock_get_metadata_last_run.return_value = ["run789", "run456"]

        # Act
        channel.send()

        # Assert
        mock_create_metadata.assert_not_called()  # This should be called only if setup_db is True
        mock_get_metadata_last_run.Assert_called_once()
        mock_send_metadata.assert_called_once()
        mock_dump.assert_called_once()
        mock_add_fk.assert_not_called()  # This should be called only if setup_db is True
        mock_remove_run.assert_called_once()

    @patch("cosmotech.coal.store.output.postgres_channel.create_metadata")
    @patch("cosmotech.coal.store.output.postgres_channel.get_metadata_last_run")
    @patch("cosmotech.coal.store.output.postgres_channel.dump_store_to_postgresql_from_conf")
    @patch("cosmotech.coal.store.output.postgres_channel.send_runner_metadata_to_postgresql")
    @patch("cosmotech.coal.store.output.postgres_channel.add_fk_constraints")
    @patch("cosmotech.coal.store.output.postgres_channel.remove_run_metadata_from_postgresql")
    def test_send_without_filter(
        self,
        mock_remove_run,
        mock_add_fk,
        mock_send_metadata,
        mock_dump,
        mock_get_metadata_last_run,
        mock_create_metadata,
        base_postgres_config,
    ):
        """Test sending data without table filter."""
        channel = PostgresChannel(base_postgres_config)
        mock_get_metadata_last_run.return_value = ["run789", "run456"]

        # Act
        channel.send()

        # Assert
        base_postgres_config.setup_db = True
        mock_create_metadata.assert_called_once()
        mock_get_metadata_last_run.Assert_called_once()
        mock_send_metadata.assert_called_once()
        mock_dump.assert_called_once()
        mock_add_fk.assert_called_once()
        mock_remove_run.assert_called_once()

        # Check the arguments passed to dump_store_to_postgresql_from_conf
        call_args = mock_dump.call_args
        assert call_args.kwargs["configuration"] == base_postgres_config
        assert call_args.kwargs["selected_tables"] is None
        assert call_args.kwargs["fk_id"] == "run789"

        call_args_remove_run = mock_remove_run.call_args
        assert call_args_remove_run.args[0] == base_postgres_config
        assert call_args_remove_run.args[1] == "run456"

    @patch("cosmotech.coal.store.output.postgres_channel.create_metadata")
    @patch("cosmotech.coal.store.output.postgres_channel.get_metadata_last_run")
    @patch("cosmotech.coal.store.output.postgres_channel.dump_store_to_postgresql_from_conf")
    @patch("cosmotech.coal.store.output.postgres_channel.send_runner_metadata_to_postgresql")
    @patch("cosmotech.coal.store.output.postgres_channel.add_fk_constraints")
    @patch("cosmotech.coal.store.output.postgres_channel.remove_run_metadata_from_postgresql")
    def test_send_without_filter(
        self,
        mock_remove_run,
        mock_add_fk,
        mock_send_metadata,
        mock_dump,
        mock_get_metadata_last_run,
        mock_create_metadata,
        base_postgres_config,
    ):
        """Test sending data without table filter."""
        channel = PostgresChannel(base_postgres_config)
        mock_get_metadata_last_run.return_value = ["run789", "run456"]

        # Act
        channel.send()

        # Assert
        base_postgres_config.setup_db = True
        mock_create_metadata.assert_called_once()
        mock_get_metadata_last_run.Assert_called_once()
        mock_send_metadata.assert_called_once()
        mock_dump.assert_called_once()
        mock_add_fk.assert_called_once()
        mock_remove_run.assert_called_once()

        # Check the arguments passed to dump_store_to_postgresql_from_conf
        call_args = mock_dump.call_args
        assert call_args.kwargs["configuration"] == base_postgres_config
        assert call_args.kwargs["selected_tables"] is None
        assert call_args.kwargs["fk_id"] == "run789"

        call_args_remove_run = mock_remove_run.call_args
        assert call_args_remove_run.args[0] == base_postgres_config
        assert call_args_remove_run.args[1] == "run456"

    @patch("cosmotech.coal.store.output.postgres_channel.create_metadata")
    @patch("cosmotech.coal.store.output.postgres_channel.get_metadata_last_run")
    @patch("cosmotech.coal.store.output.postgres_channel.dump_store_to_postgresql_from_conf")
    @patch("cosmotech.coal.store.output.postgres_channel.send_runner_metadata_to_postgresql")
    @patch("cosmotech.coal.store.output.postgres_channel.add_fk_constraints")
    @patch("cosmotech.coal.store.output.postgres_channel.remove_run_metadata_from_postgresql")
    def test_send_with_filter(
        self,
        mock_remove_run,
        mock_add_fk,
        mock_send_metadata,
        mock_dump,
        mock_get_metadata_last_run,
        mock_create_metadata,
        base_postgres_config,
    ):
        """Test sending data with table filter."""
        channel = PostgresChannel(base_postgres_config)
        tables_filter = ["table1", "table2", "table3"]
        mock_get_metadata_last_run.return_value = ["run789", "run456"]

        # Act
        channel.send(filter=tables_filter)

        # Assert
        base_postgres_config.setup_db = True
        mock_create_metadata.assert_called_once()
        mock_get_metadata_last_run.Assert_called_once()
        mock_send_metadata.assert_called_once()
        mock_dump.assert_called_once()
        mock_add_fk.assert_called_once()
        mock_remove_run.assert_called_once()

        # Check the arguments passed to dump_store_to_postgresql_from_conf
        call_args = mock_dump.call_args
        assert call_args.kwargs["configuration"] == base_postgres_config
        assert call_args.kwargs["selected_tables"] == ["table1", "table2", "table3"]
        assert call_args.kwargs["fk_id"] == "run789"

        call_args_remove_run = mock_remove_run.call_args
        assert call_args_remove_run.args[0] == base_postgres_config
        assert call_args_remove_run.args[1] == "run456"

    @patch("cosmotech.coal.store.output.postgres_channel.remove_runner_metadata_from_postgresql")
    def test_delete(self, mock_remove_metadata, base_postgres_config):
        """Test delete method."""

        channel = PostgresChannel(base_postgres_config)

        # Act
        channel.delete()

        # Assert
        base_postgres_config.setup_db = True
        mock_remove_metadata.assert_called_once()
        # Check that configuration was passed
        call_args = mock_remove_metadata.call_args
        assert call_args.args[0] == base_postgres_config
