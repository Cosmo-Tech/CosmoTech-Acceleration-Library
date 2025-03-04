# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
from unittest.mock import MagicMock, patch

from cosmotech.coal.postgresql.runner import send_runner_metadata_to_postgresql


class TestRunnerFunctions:
    """Tests for top-level functions in the runner module."""

    @patch("cosmotech.coal.postgresql.runner.get_api_client")
    @patch("cosmotech.coal.postgresql.runner.get_runner_metadata")
    @patch("cosmotech.coal.postgresql.runner.generate_postgresql_full_uri")
    @patch("cosmotech.coal.postgresql.runner.dbapi.connect")
    def test_send_runner_metadata_to_postgresql(
        self, mock_connect, mock_generate_uri, mock_get_runner_metadata, mock_get_api_client
    ):
        """Test the send_runner_metadata_to_postgresql function."""
        # Arrange
        # Mock API client with context manager behavior
        mock_api_client = MagicMock()
        mock_api_client_context = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client_context
        mock_get_api_client.return_value = (mock_api_client, "Test Connection")

        # Mock runner metadata
        mock_runner = {
            "id": "test-runner-id",
            "name": "Test Runner",
            "lastRunId": "test-run-id",
            "runTemplateId": "test-template-id",
        }
        mock_get_runner_metadata.return_value = mock_runner

        # Mock PostgreSQL URI
        mock_uri = "postgresql://user:password@host:5432/db"
        mock_generate_uri.return_value = mock_uri

        # Mock PostgreSQL connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn

        # Test parameters
        organization_id = "test-org"
        workspace_id = "test-workspace"
        runner_id = "test-runner-id"
        postgres_host = "localhost"
        postgres_port = 5432
        postgres_db = "testdb"
        postgres_schema = "public"
        postgres_user = "user"
        postgres_password = "password"
        table_prefix = "Test_"

        # Act
        send_runner_metadata_to_postgresql(
            organization_id,
            workspace_id,
            runner_id,
            postgres_host,
            postgres_port,
            postgres_db,
            postgres_schema,
            postgres_user,
            postgres_password,
            table_prefix,
        )

        # Assert
        # Check that API client was used correctly
        mock_get_api_client.assert_called_once()
        mock_get_runner_metadata.assert_called_once_with(
            mock_api_client_context, organization_id, workspace_id, runner_id
        )

        # Check that PostgreSQL URI was generated correctly
        mock_generate_uri.assert_called_once_with(
            postgres_host, postgres_port, postgres_db, postgres_user, postgres_password
        )

        # Check that PostgreSQL connection was established
        mock_connect.assert_called_once_with(mock_uri, autocommit=True)

        # Check that SQL statements were executed
        assert mock_cursor.execute.call_count == 2

        # Verify the SQL statements (partially, since the exact SQL is complex)
        create_table_call = mock_cursor.execute.call_args_list[0]
        assert "CREATE TABLE IF NOT EXISTS" in create_table_call[0][0]
        assert f"{postgres_schema}.{table_prefix}RunnerMetadata" in create_table_call[0][0]

        upsert_call = mock_cursor.execute.call_args_list[1]
        assert "INSERT INTO" in upsert_call[0][0]
        assert f"{postgres_schema}.{table_prefix}RunnerMetadata" in upsert_call[0][0]
        assert upsert_call[0][1] == (
            mock_runner["id"],
            mock_runner["name"],
            mock_runner["lastRunId"],
            mock_runner["runTemplateId"],
        )

        # Check that commits were called
        assert mock_conn.commit.call_count == 2
