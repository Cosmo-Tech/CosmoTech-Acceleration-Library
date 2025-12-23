# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import MagicMock, patch

from cosmotech.coal.postgresql.runner import (
    remove_runner_metadata_from_postgresql,
    send_runner_metadata_to_postgresql,
)


class TestRunnerFunctions:
    """Tests for top-level functions in the runner module."""

    @patch("cosmotech.coal.postgresql.runner.RunnerApi")
    @patch("cosmotech.coal.postgresql.runner.PostgresUtils")
    @patch("cosmotech.coal.postgresql.runner.dbapi.connect")
    def test_send_runner_metadata_to_postgresql(self, mock_connect, mock_postgres_utils_class, mock_runner_api_class):
        """Test the send_runner_metadata_to_postgresql function."""
        # Arrange

        # Mock Configuration
        mock_configuration = MagicMock()
        mock_configuration.cosmotech.organization_id = "test-org"
        mock_configuration.cosmotech.workspace_id = "test-workspace"
        mock_configuration.cosmotech.runner_id = "test-runner-id"

        # Mock runner metadata
        mock_runner = {
            "id": "test-runner-id",
            "name": "Test Runner",
            "lastRunInfo": {"lastRunId": "test-run-id"},
            "runTemplateId": "test-template-id",
        }

        # Mock RunnerApi instance
        mock_runner_api_instance = MagicMock()
        mock_runner_api_instance.get_runner_metadata.return_value = mock_runner
        mock_runner_api_class.return_value = mock_runner_api_instance

        # Mock PostgresUtils instance
        mock_postgres_utils_instance = MagicMock()
        mock_postgres_utils_instance.full_uri = "postgresql://user:password@localhost:5432/testdb"
        mock_postgres_utils_instance.db_schema = "public"
        mock_postgres_utils_instance.table_prefix = "Test_"
        mock_postgres_utils_class.return_value = mock_postgres_utils_instance

        # Mock PostgreSQL connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn

        # Act
        result = send_runner_metadata_to_postgresql(mock_configuration)

        # Assert
        # Verify PostgresUtils was instantiated with configuration
        mock_postgres_utils_class.assert_called_once_with(mock_configuration)

        # Verify RunnerApi was instantiated with configuration
        mock_runner_api_class.assert_called_once_with(mock_configuration)

        # Verify get_runner_metadata was called with correct parameters
        mock_runner_api_instance.get_runner_metadata.assert_called_once_with("test-runner-id")

        # Verify PostgreSQL connection was established
        mock_connect.assert_called_once_with("postgresql://user:password@localhost:5432/testdb", autocommit=True)

        # Check that SQL statements were executed
        assert mock_cursor.execute.call_count == 2

        # Verify the SQL statements (partially, since the exact SQL is complex)
        create_table_call = mock_cursor.execute.call_args_list[0]
        assert "CREATE TABLE IF NOT EXISTS" in create_table_call[0][0]
        assert "public.Test_RunnerMetadata" in create_table_call[0][0]

        upsert_call = mock_cursor.execute.call_args_list[1]
        assert "INSERT INTO" in upsert_call[0][0]
        assert "public.Test_RunnerMetadata" in upsert_call[0][0]
        assert upsert_call[0][1] == (
            mock_runner["id"],
            mock_runner["name"],
            mock_runner["lastRunInfo"]["lastRunId"],
            mock_runner["runTemplateId"],
        )

        # Check that commits were called
        assert mock_conn.commit.call_count == 2

        # Verify the function returns the lastRunId
        assert result == "test-run-id"

    @patch("cosmotech.coal.postgresql.runner.RunnerApi")
    @patch("cosmotech.coal.postgresql.runner.PostgresUtils")
    @patch("cosmotech.coal.postgresql.runner.dbapi.connect")
    def test_remove_runner_metadata_to_postgresql(self, mock_connect, mock_postgres_utils_class, mock_runner_api_class):
        """Test the remove_runner_metadata_from_postgresql function."""
        # Arrange

        # Mock Configuration
        mock_configuration = MagicMock()
        mock_configuration.cosmotech.organization_id = "test-org"
        mock_configuration.cosmotech.workspace_id = "test-workspace"
        mock_configuration.cosmotech.runner_id = "test-runner-id"

        # Mock runner metadata
        mock_runner = {
            "id": "test-runner-id",
            "name": "Test Runner",
            "lastRunId": "test-run-id",
            "runTemplateId": "test-template-id",
        }

        # Mock RunnerApi instance
        mock_runner_api_instance = MagicMock()
        mock_runner_api_instance.get_runner_metadata.return_value = mock_runner
        mock_runner_api_class.return_value = mock_runner_api_instance

        # Mock PostgresUtils instance
        mock_postgres_utils_instance = MagicMock()
        mock_postgres_utils_instance.full_uri = "postgresql://user:password@localhost:5432/testdb"
        mock_postgres_utils_instance.db_schema = "public"
        mock_postgres_utils_instance.table_prefix = "Test_"
        mock_postgres_utils_class.return_value = mock_postgres_utils_instance

        # Mock PostgreSQL connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn

        # Act
        result = remove_runner_metadata_from_postgresql(mock_configuration)

        # Assert
        # Verify PostgresUtils was instantiated with configuration
        mock_postgres_utils_class.assert_called_once_with(mock_configuration)

        # Verify RunnerApi was instantiated with configuration
        mock_runner_api_class.assert_called_once_with(mock_configuration)

        # Verify get_runner_metadata was called with correct parameters
        mock_runner_api_instance.get_runner_metadata.assert_called_once_with("test-runner-id")

        # Check that PostgreSQL connection was established
        mock_connect.assert_called_once_with("postgresql://user:password@localhost:5432/testdb", autocommit=True)

        # Check that SQL statements were executed
        assert mock_cursor.execute.call_count == 1

        # Verify the SQL statements (partially, since the exact SQL is complex)
        delete_call = mock_cursor.execute.call_args_list[0]
        assert "DELETE FROM" in delete_call[0][0]
        assert "public.Test_RunnerMetadata" in delete_call[0][0]

        # Check that commits were called
        assert mock_conn.commit.call_count == 1

        # Verify the function returns the lastRunId
        assert result == "test-run-id"
