# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import time
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call

from azure.kusto.data import KustoClient
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, ReportLevel
from azure.kusto.ingest.status import KustoIngestStatusQueues, SuccessMessage, FailureMessage

from cosmotech.coal.azure.adx.ingestion import (
    ingest_dataframe,
    send_to_adx,
    check_ingestion_status,
    clear_ingestion_status_queues,
    IngestionStatus,
    _ingest_status,
    _ingest_times,
)


class TestIngestionFunctions:
    """Tests for top-level functions in the ingestion module."""

    @pytest.fixture
    def mock_ingest_client(self):
        """Create a mock QueuedIngestClient."""
        return MagicMock(spec=QueuedIngestClient)

    @pytest.fixture
    def mock_kusto_client(self):
        """Create a mock KustoClient."""
        return MagicMock(spec=KustoClient)

    @pytest.fixture
    def mock_dataframe(self):
        """Create a mock pandas DataFrame."""
        return pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [10.5, 20.3, 30.1]})

    @pytest.fixture
    def mock_ingestion_result(self):
        """Create a mock ingestion result."""
        mock_result = MagicMock()
        mock_result.source_id = "test-source-id"
        return mock_result

    @pytest.fixture
    def mock_status_queues(self):
        """Create a mock KustoIngestStatusQueues."""
        mock_queues = MagicMock(spec=KustoIngestStatusQueues)
        mock_success_queue = MagicMock()
        mock_failure_queue = MagicMock()
        mock_queues.success = mock_success_queue
        mock_queues.failure = mock_failure_queue
        return mock_queues

    def test_ingest_dataframe(self, mock_ingest_client, mock_dataframe, mock_ingestion_result):
        """Test the ingest_dataframe function."""
        # Arrange
        database = "test-database"
        table_name = "test-table"
        drop_by_tag = "test-tag"

        mock_ingest_client.ingest_from_dataframe.return_value = mock_ingestion_result

        # Act
        result = ingest_dataframe(mock_ingest_client, database, table_name, mock_dataframe, drop_by_tag)

        # Assert
        mock_ingest_client.ingest_from_dataframe.assert_called_once()

        # Verify the ingestion properties
        call_args = mock_ingest_client.ingest_from_dataframe.call_args
        # The dataframe is passed as the first positional argument
        assert call_args[0][0] is mock_dataframe

        ingestion_props = call_args[1]["ingestion_properties"]
        assert ingestion_props.database == database
        assert ingestion_props.table == table_name
        assert ingestion_props.drop_by_tags == [drop_by_tag]

        # Verify the result
        assert result == mock_ingestion_result

        # Verify the ingestion status tracking
        source_id = str(mock_ingestion_result.source_id)
        assert source_id in _ingest_status
        assert _ingest_status[source_id] == IngestionStatus.QUEUED
        assert source_id in _ingest_times

    def test_ingest_dataframe_no_drop_by_tag(self, mock_ingest_client, mock_dataframe, mock_ingestion_result):
        """Test the ingest_dataframe function without a drop_by_tag."""
        # Arrange
        database = "test-database"
        table_name = "test-table"

        mock_ingest_client.ingest_from_dataframe.return_value = mock_ingestion_result

        # Act
        result = ingest_dataframe(mock_ingest_client, database, table_name, mock_dataframe)

        # Assert
        mock_ingest_client.ingest_from_dataframe.assert_called_once()

        # Verify the ingestion properties
        call_args = mock_ingest_client.ingest_from_dataframe.call_args
        ingestion_props = call_args[1]["ingestion_properties"]
        assert ingestion_props.drop_by_tags is None

    @patch("cosmotech.coal.azure.adx.ingestion.ingest_dataframe")
    def test_send_to_adx_with_data(
        self, mock_ingest_dataframe, mock_kusto_client, mock_ingest_client, mock_ingestion_result
    ):
        """Test the send_to_adx function with data."""
        # Arrange
        database = "test-database"
        table_name = "test-table"
        dict_list = [
            {"id": 1, "name": "Alice", "value": 10.5},
            {"id": 2, "name": "Bob", "value": 20.3},
        ]
        drop_by_tag = "test-tag"

        mock_ingest_dataframe.return_value = mock_ingestion_result

        # Act
        result = send_to_adx(
            mock_kusto_client,
            mock_ingest_client,
            database,
            dict_list,
            table_name,
            ignore_table_creation=True,
            drop_by_tag=drop_by_tag,
        )

        # Assert
        # Verify that ingest_dataframe was called with the correct parameters
        mock_ingest_dataframe.assert_called_once()
        call_args = mock_ingest_dataframe.call_args
        assert call_args[0][0] == mock_ingest_client
        assert call_args[0][1] == database
        assert call_args[0][2] == table_name
        assert isinstance(call_args[0][3], pd.DataFrame)
        assert call_args[0][4] == drop_by_tag

        # Verify the result
        assert result == mock_ingestion_result

    def test_send_to_adx_empty_list(self, mock_kusto_client, mock_ingest_client):
        """Test the send_to_adx function with an empty list."""
        # Arrange
        database = "test-database"
        table_name = "test-table"
        dict_list = []

        # Act
        result = send_to_adx(mock_kusto_client, mock_ingest_client, database, dict_list, table_name)

        # Assert
        assert result is None

    @patch("cosmotech.coal.azure.adx.ingestion.create_table")
    @patch("cosmotech.coal.azure.adx.ingestion.ingest_dataframe")
    def test_send_to_adx_create_table(
        self, mock_ingest_dataframe, mock_create_table, mock_kusto_client, mock_ingest_client, mock_ingestion_result
    ):
        """Test the send_to_adx function with table creation."""
        # Arrange
        database = "test-database"
        table_name = "test-table"
        dict_list = [
            {"id": 1, "name": "Alice", "value": 10.5},
        ]

        mock_create_table.return_value = True
        mock_ingest_dataframe.return_value = mock_ingestion_result

        # Act
        result = send_to_adx(
            mock_kusto_client, mock_ingest_client, database, dict_list, table_name, ignore_table_creation=False
        )

        # Assert
        # Verify that create_table was called with the correct parameters
        mock_create_table.assert_called_once()
        assert mock_create_table.call_args[0][0] == mock_kusto_client
        assert mock_create_table.call_args[0][1] == database
        assert mock_create_table.call_args[0][2] == table_name

        # Verify that ingest_dataframe was called
        mock_ingest_dataframe.assert_called_once()

        # Verify the result
        assert result == mock_ingestion_result

    @patch("cosmotech.coal.azure.adx.ingestion.create_table")
    def test_send_to_adx_table_creation_failed(self, mock_create_table, mock_kusto_client, mock_ingest_client):
        """Test the send_to_adx function when table creation fails."""
        # Arrange
        database = "test-database"
        table_name = "test-table"
        dict_list = [
            {"id": 1, "name": "Alice", "value": 10.5},
        ]

        mock_create_table.return_value = False

        # Act
        result = send_to_adx(
            mock_kusto_client, mock_ingest_client, database, dict_list, table_name, ignore_table_creation=False
        )

        # Assert
        # Verify that create_table was called
        mock_create_table.assert_called_once()

        # Verify the result
        assert result is False

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_check_ingestion_status_already_known(self, mock_status_queues_class, mock_ingest_client):
        """Test the check_ingestion_status function with already known statuses."""
        # Arrange
        source_id1 = "source-id-1"
        source_id2 = "source-id-2"
        source_id3 = "source-id-3"

        # Set up known statuses and times
        _ingest_status[source_id1] = IngestionStatus.SUCCESS
        _ingest_status[source_id2] = IngestionStatus.FAILURE
        _ingest_status[source_id3] = IngestionStatus.QUEUED

        # Make sure _ingest_times is initialized for all source IDs
        _ingest_times[source_id1] = time.time()
        _ingest_times[source_id2] = time.time()
        _ingest_times[source_id3] = time.time()

        # Act
        result = list(check_ingestion_status(mock_ingest_client, [source_id1, source_id2, source_id3]))

        # Assert
        # Verify that KustoIngestStatusQueues was called for the queued status
        mock_status_queues_class.assert_called_once_with(mock_ingest_client)

        # Verify the results
        assert len(result) == 3
        assert (source_id1, IngestionStatus.SUCCESS) in result
        assert (source_id2, IngestionStatus.FAILURE) in result
        assert (source_id3, IngestionStatus.QUEUED) in result or (source_id3, IngestionStatus.UNKNOWN) in result

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_check_ingestion_status_with_success_message(
        self, mock_status_queues_class, mock_ingest_client, mock_status_queues
    ):
        """Test the check_ingestion_status function with a success message."""
        # Arrange
        source_id = "source-id-success"
        _ingest_status[source_id] = IngestionStatus.QUEUED
        _ingest_times[source_id] = time.time()

        # Set up mock status queues
        mock_status_queues_class.return_value = mock_status_queues

        # Create mock success queue and message
        mock_success_queue = MagicMock()
        mock_message = MagicMock()
        mock_message.content = '{"IngestionSourceId": "source-id-success"}'

        # Set up the success queue to return our message
        mock_success_queue.receive_messages.return_value = [mock_message]
        mock_status_queues.success._get_queues.return_value = [mock_success_queue]

        # Set up empty failure queue
        mock_failure_queue = MagicMock()
        mock_failure_queue.receive_messages.return_value = []
        mock_status_queues.failure._get_queues.return_value = [mock_failure_queue]

        # Act
        with patch(
            "cosmotech.coal.azure.adx.ingestion.SuccessMessage", return_value=MagicMock(IngestionSourceId=source_id)
        ):
            result = list(check_ingestion_status(mock_ingest_client, [source_id]))

        # Assert
        assert len(result) == 1
        assert result[0] == (source_id, IngestionStatus.SUCCESS)
        mock_success_queue.delete_message.assert_called_once_with(mock_message)

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_check_ingestion_status_with_failure_message(
        self, mock_status_queues_class, mock_ingest_client, mock_status_queues
    ):
        """Test the check_ingestion_status function with a failure message."""
        # Arrange
        source_id = "source-id-failure"
        _ingest_status[source_id] = IngestionStatus.QUEUED
        _ingest_times[source_id] = time.time()

        # Set up mock status queues
        mock_status_queues_class.return_value = mock_status_queues

        # Create empty success queue
        mock_success_queue = MagicMock()
        mock_success_queue.receive_messages.return_value = []
        mock_status_queues.success._get_queues.return_value = [mock_success_queue]

        # Create mock failure queue and message
        mock_failure_queue = MagicMock()
        mock_message = MagicMock()
        mock_message.content = '{"IngestionSourceId": "source-id-failure"}'

        # Set up the failure queue to return our message
        mock_failure_queue.receive_messages.return_value = [mock_message]
        mock_status_queues.failure._get_queues.return_value = [mock_failure_queue]

        # Act
        with patch(
            "cosmotech.coal.azure.adx.ingestion.FailureMessage", return_value=MagicMock(IngestionSourceId=source_id)
        ):
            result = list(check_ingestion_status(mock_ingest_client, [source_id]))

        # Assert
        assert len(result) == 1
        assert result[0] == (source_id, IngestionStatus.FAILURE)
        mock_failure_queue.delete_message.assert_called_once_with(mock_message)

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_check_ingestion_status_with_timeout(
        self, mock_status_queues_class, mock_ingest_client, mock_status_queues
    ):
        """Test the check_ingestion_status function with a timeout."""
        # Arrange
        source_id = "source-id-timeout"
        _ingest_status[source_id] = IngestionStatus.QUEUED
        _ingest_times[source_id] = time.time() - 10  # 10 seconds ago

        # Set up mock status queues with empty queues
        mock_status_queues_class.return_value = mock_status_queues
        mock_success_queue = MagicMock()
        mock_success_queue.receive_messages.return_value = []
        mock_status_queues.success._get_queues.return_value = [mock_success_queue]
        mock_failure_queue = MagicMock()
        mock_failure_queue.receive_messages.return_value = []
        mock_status_queues.failure._get_queues.return_value = [mock_failure_queue]

        # Act
        result = list(check_ingestion_status(mock_ingest_client, [source_id], timeout=5))  # 5 second timeout

        # Assert
        assert len(result) == 1
        assert result[0] == (source_id, IngestionStatus.TIMEOUT)

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_check_ingestion_status_with_logs(self, mock_status_queues_class, mock_ingest_client, mock_status_queues):
        """Test the check_ingestion_status function with logs enabled."""
        # Arrange
        source_id = "source-id-logs"
        _ingest_status[source_id] = IngestionStatus.QUEUED
        _ingest_times[source_id] = time.time()

        # Set up mock status queues with empty queues
        mock_status_queues_class.return_value = mock_status_queues
        mock_success_queue = MagicMock()
        mock_success_queue.receive_messages.return_value = []
        mock_status_queues.success._get_queues.return_value = [mock_success_queue]
        mock_failure_queue = MagicMock()
        mock_failure_queue.receive_messages.return_value = []
        mock_status_queues.failure._get_queues.return_value = [mock_failure_queue]

        # Act
        result = list(check_ingestion_status(mock_ingest_client, [source_id], logs=True))

        # Assert
        assert len(result) == 1
        # The status should still be QUEUED since no messages were found and no timeout occurred
        assert result[0] == (source_id, IngestionStatus.QUEUED)

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_check_ingestion_status_unknown_id(self, mock_status_queues_class, mock_ingest_client, mock_status_queues):
        """Test the check_ingestion_status function with an unknown source ID."""
        # Arrange
        source_id = "unknown-source-id"
        # Don't initialize _ingest_status or _ingest_times for this ID

        # Set up mock status queues with empty queues
        mock_status_queues_class.return_value = mock_status_queues
        mock_success_queue = MagicMock()
        mock_success_queue.receive_messages.return_value = []
        mock_status_queues.success._get_queues.return_value = [mock_success_queue]
        mock_failure_queue = MagicMock()
        mock_failure_queue.receive_messages.return_value = []
        mock_status_queues.failure._get_queues.return_value = [mock_failure_queue]

        # Act
        result = list(check_ingestion_status(mock_ingest_client, [source_id]))

        # Assert
        assert len(result) == 1
        assert result[0] == (source_id, IngestionStatus.UNKNOWN)
        # Verify that the ID was added to the tracking dictionaries
        assert source_id in _ingest_status
        assert source_id in _ingest_times

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_clear_ingestion_status_queues_with_confirmation(
        self, mock_status_queues_class, mock_ingest_client, mock_status_queues
    ):
        """Test the clear_ingestion_status_queues function with confirmation."""
        # Arrange
        mock_status_queues_class.return_value = mock_status_queues
        mock_status_queues.success.is_empty.side_effect = [
            False,
            True,
        ]  # First call returns False, second call returns True
        mock_status_queues.failure.is_empty.side_effect = [
            False,
            True,
        ]  # First call returns False, second call returns True

        # Act
        clear_ingestion_status_queues(mock_ingest_client, confirmation=True)

        # Assert
        # Verify that the queues were cleared
        mock_status_queues.success.pop.assert_called_once_with(32)
        mock_status_queues.failure.pop.assert_called_once_with(32)

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_clear_ingestion_status_queues_without_confirmation(
        self, mock_status_queues_class, mock_ingest_client, mock_status_queues
    ):
        """Test the clear_ingestion_status_queues function without confirmation."""
        # Arrange
        mock_status_queues_class.return_value = mock_status_queues

        # Act
        clear_ingestion_status_queues(mock_ingest_client, confirmation=False)

        # Assert
        # Verify that the queues were not cleared
        mock_status_queues.success.pop.assert_not_called()
        mock_status_queues.failure.pop.assert_not_called()
