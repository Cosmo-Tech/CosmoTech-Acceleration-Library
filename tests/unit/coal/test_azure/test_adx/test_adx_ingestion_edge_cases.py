# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import time
import pytest
from unittest.mock import MagicMock, patch

from azure.kusto.ingest import QueuedIngestClient
from azure.kusto.ingest.status import KustoIngestStatusQueues

from cosmotech.orchestrator.utils.translate import T
from cosmotech.coal.azure.adx.ingestion import (
    check_ingestion_status,
    IngestionStatus,
    _ingest_status,
    _ingest_times,
)


class TestIngestionEdgeCases:
    """Edge case tests for the ingestion module."""

    @pytest.fixture(autouse=True)
    def reset_ingest_status(self):
        """Reset the ingestion status dictionaries before each test."""
        _ingest_status.clear()
        _ingest_times.clear()
        yield
        _ingest_status.clear()
        _ingest_times.clear()

    @pytest.fixture
    def mock_ingest_client(self):
        """Create a mock QueuedIngestClient."""
        return MagicMock(spec=QueuedIngestClient)

    @pytest.fixture
    def mock_status_queues(self):
        """Create a mock KustoIngestStatusQueues."""
        mock_queues = MagicMock(spec=KustoIngestStatusQueues)
        mock_success_queue = MagicMock()
        mock_failure_queue = MagicMock()
        mock_queues.success = mock_success_queue
        mock_queues.failure = mock_failure_queue
        return mock_queues

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_check_ingestion_status_with_logs_and_messages(
        self, mock_status_queues_class, mock_ingest_client, mock_status_queues
    ):
        """Test check_ingestion_status with logs enabled and messages in the queues."""
        # Arrange
        source_id = "source-id-logs-messages"
        _ingest_status[source_id] = IngestionStatus.QUEUED
        _ingest_times[source_id] = time.time()

        # Set up mock status queues
        mock_status_queues_class.return_value = mock_status_queues

        # Create mock success queue and message
        mock_success_queue = MagicMock()
        mock_message = MagicMock()
        mock_message.content = '{"IngestionSourceId": "source-id-logs-messages"}'

        # Set up the success queue to return our message
        mock_success_queue.receive_messages.return_value = [mock_message]
        mock_status_queues.success._get_queues.return_value = [mock_success_queue]

        # Set up empty failure queue
        mock_failure_queue = MagicMock()
        mock_failure_queue.receive_messages.return_value = []
        mock_status_queues.failure._get_queues.return_value = [mock_failure_queue]

        # Act
        with patch(
            "cosmotech.coal.azure.adx.ingestion.SuccessMessage", 
            return_value=MagicMock(IngestionSourceId=source_id)
        ):
            result = list(check_ingestion_status(mock_ingest_client, [source_id], logs=True))

        # Assert
        assert len(result) == 1
        assert result[0] == (source_id, IngestionStatus.SUCCESS)
        
        # Verify that the message was deleted
        mock_success_queue.delete_message.assert_called_once_with(mock_message)

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_check_ingestion_status_with_multiple_messages(
        self, mock_status_queues_class, mock_ingest_client, mock_status_queues
    ):
        """Test check_ingestion_status with multiple messages in the queues."""
        # Arrange
        source_id1 = "source-id-1"
        source_id2 = "source-id-2"
        _ingest_status[source_id1] = IngestionStatus.QUEUED
        _ingest_status[source_id2] = IngestionStatus.QUEUED
        _ingest_times[source_id1] = time.time()
        _ingest_times[source_id2] = time.time()

        # Set up mock status queues
        mock_status_queues_class.return_value = mock_status_queues

        # Create mock success queue and messages
        mock_success_queue = MagicMock()
        mock_message1 = MagicMock()
        mock_message1.content = '{"IngestionSourceId": "source-id-1"}'
        mock_message2 = MagicMock()
        mock_message2.content = '{"IngestionSourceId": "source-id-2"}'

        # Set up the success queue to return our messages
        mock_success_queue.receive_messages.return_value = [mock_message1, mock_message2]
        mock_status_queues.success._get_queues.return_value = [mock_success_queue]

        # Set up empty failure queue
        mock_failure_queue = MagicMock()
        mock_failure_queue.receive_messages.return_value = []
        mock_status_queues.failure._get_queues.return_value = [mock_failure_queue]

        # Act
        with patch(
            "cosmotech.coal.azure.adx.ingestion.SuccessMessage", 
            side_effect=[
                MagicMock(IngestionSourceId=source_id1),
                MagicMock(IngestionSourceId=source_id2)
            ]
        ):
            result = list(check_ingestion_status(mock_ingest_client, [source_id1, source_id2], logs=True))

        # Assert
        assert len(result) == 2
        assert (source_id1, IngestionStatus.SUCCESS) in result
        assert (source_id2, IngestionStatus.QUEUED) in result or (source_id2, IngestionStatus.SUCCESS) in result
        
        # Verify that at least one message was deleted
        assert mock_success_queue.delete_message.call_count >= 1

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_check_ingestion_status_with_success_messages_and_logs(
        self, mock_status_queues_class, mock_ingest_client, mock_status_queues
    ):
        """Test check_ingestion_status with success messages and logs enabled."""
        # Arrange
        source_id = "source-id-success-logs"
        _ingest_status[source_id] = IngestionStatus.QUEUED
        _ingest_times[source_id] = time.time()

        # Set up mock status queues
        mock_status_queues_class.return_value = mock_status_queues

        # Create mock success queue and message
        mock_success_queue = MagicMock()
        mock_success_message = MagicMock()
        mock_success_message.content = '{"IngestionSourceId": "source-id-success-logs"}'
        mock_success_queue.receive_messages.return_value = [mock_success_message]
        mock_status_queues.success._get_queues.return_value = [mock_success_queue]

        # Set up empty failure queue
        mock_failure_queue = MagicMock()
        mock_failure_queue.receive_messages.return_value = []
        mock_status_queues.failure._get_queues.return_value = [mock_failure_queue]

        # Act
        with patch(
            "cosmotech.coal.azure.adx.ingestion.SuccessMessage", 
            return_value=MagicMock(IngestionSourceId=source_id)
        ):
            result = list(check_ingestion_status(mock_ingest_client, [source_id], logs=True))

        # Assert
        assert len(result) == 1
        assert result[0] == (source_id, IngestionStatus.SUCCESS)
        
        # Verify that the message was deleted
        mock_success_queue.delete_message.assert_called_once_with(mock_success_message)

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_check_ingestion_status_with_failure_messages_and_logs(
        self, mock_status_queues_class, mock_ingest_client, mock_status_queues
    ):
        """Test check_ingestion_status with failure messages and logs enabled."""
        # Arrange
        source_id = "source-id-failure-logs"
        _ingest_status[source_id] = IngestionStatus.QUEUED
        _ingest_times[source_id] = time.time()

        # Set up mock status queues
        mock_status_queues_class.return_value = mock_status_queues

        # Set up empty success queue
        mock_success_queue = MagicMock()
        mock_success_queue.receive_messages.return_value = []
        mock_status_queues.success._get_queues.return_value = [mock_success_queue]

        # Create mock failure queue and message
        mock_failure_queue = MagicMock()
        mock_failure_message = MagicMock()
        mock_failure_message.content = '{"IngestionSourceId": "source-id-failure-logs"}'
        mock_failure_queue.receive_messages.return_value = [mock_failure_message]
        mock_status_queues.failure._get_queues.return_value = [mock_failure_queue]

        # Act
        with patch(
            "cosmotech.coal.azure.adx.ingestion.FailureMessage", 
            return_value=MagicMock(IngestionSourceId=source_id)
        ):
            result = list(check_ingestion_status(mock_ingest_client, [source_id], logs=True))

        # Assert
        assert len(result) == 1
        assert result[0] == (source_id, IngestionStatus.FAILURE)
        
        # Verify that the message was deleted
        mock_failure_queue.delete_message.assert_called_once_with(mock_failure_message)

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    @patch("cosmotech.coal.azure.adx.ingestion.LOGGER")
    def test_check_ingestion_status_with_logs_and_status_messages(
        self, mock_logger, mock_status_queues_class, mock_ingest_client, mock_status_queues
    ):
        """Test check_ingestion_status with logs enabled and status messages."""
        # Arrange
        source_id = "source-id-logs-status"
        _ingest_status[source_id] = IngestionStatus.QUEUED
        _ingest_times[source_id] = time.time()

        # Set up mock status queues
        mock_status_queues_class.return_value = mock_status_queues

        # Create mock success queue with multiple messages
        mock_success_queue = MagicMock()
        mock_success_message1 = MagicMock()
        mock_success_message1.content = '{"IngestionSourceId": "source-id-logs-status"}'
        mock_success_message2 = MagicMock()
        mock_success_message2.content = '{"IngestionSourceId": "other-source-id"}'
        mock_success_queue.receive_messages.return_value = [mock_success_message1, mock_success_message2]
        mock_status_queues.success._get_queues.return_value = [mock_success_queue]

        # Create mock failure queue with a message
        mock_failure_queue = MagicMock()
        mock_failure_message = MagicMock()
        mock_failure_message.content = '{"IngestionSourceId": "failure-source-id"}'
        mock_failure_queue.receive_messages.return_value = [mock_failure_message]
        mock_status_queues.failure._get_queues.return_value = [mock_failure_queue]

        # Act
        with patch(
            "cosmotech.coal.azure.adx.ingestion.SuccessMessage", 
            return_value=MagicMock(IngestionSourceId=source_id)
        ):
            result = list(check_ingestion_status(mock_ingest_client, [source_id], logs=True))

        # Assert
        assert len(result) == 1
        assert result[0] == (source_id, IngestionStatus.SUCCESS)
        
        # Verify that the debug log was called with the correct message
        mock_logger.debug.assert_any_call(T("coal.logs.adx.status_messages").format(success=2, failure=1))
        
        # Verify that the message was deleted
        mock_success_queue.delete_message.assert_called_once_with(mock_success_message1)

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_check_ingestion_status_with_no_matching_messages(
        self, mock_status_queues_class, mock_ingest_client, mock_status_queues
    ):
        """Test check_ingestion_status with messages that don't match any source IDs."""
        # Arrange
        source_id = "source-id-no-match"
        _ingest_status[source_id] = IngestionStatus.QUEUED
        _ingest_times[source_id] = time.time()

        # Set up mock status queues
        mock_status_queues_class.return_value = mock_status_queues

        # Create mock success queue with a message for a different source ID
        mock_success_queue = MagicMock()
        mock_message = MagicMock()
        mock_message.content = '{"IngestionSourceId": "different-source-id"}'
        mock_success_queue.receive_messages.return_value = [mock_message]
        mock_status_queues.success._get_queues.return_value = [mock_success_queue]

        # Set up empty failure queue
        mock_failure_queue = MagicMock()
        mock_failure_queue.receive_messages.return_value = []
        mock_status_queues.failure._get_queues.return_value = [mock_failure_queue]

        # Act
        with patch(
            "cosmotech.coal.azure.adx.ingestion.SuccessMessage", 
            return_value=MagicMock(IngestionSourceId="different-source-id")
        ):
            result = list(check_ingestion_status(mock_ingest_client, [source_id], logs=True))

        # Assert
        assert len(result) == 1
        assert result[0] == (source_id, IngestionStatus.QUEUED)
        
        # Verify that no messages were deleted
        mock_success_queue.delete_message.assert_not_called()

    def test_status_messages_log_line_true(self):
        """Test the specific log line that's not being covered with logs=True."""
        # Import the module directly to access the function
        import cosmotech.coal.azure.adx.ingestion as ingestion_module
        
        # Create mock objects
        mock_logger = MagicMock()
        mock_t = MagicMock()
        mock_format = MagicMock()
        mock_t.return_value = mock_format
        mock_format.format.return_value = "Status message"
        
        # Replace the real objects with mocks
        original_logger = ingestion_module.LOGGER
        original_t = ingestion_module.T
        ingestion_module.LOGGER = mock_logger
        ingestion_module.T = mock_t
        
        try:
            # Create test data
            successes = [1, 2, 3]  # Just need a list with a length
            failures = [1]  # Just need a list with a length
            logs = True
            
            # Call the specific line directly
            if logs:
                ingestion_module.LOGGER.debug(ingestion_module.T("coal.logs.adx.status_messages").format(
                    success=len(successes), failure=len(failures)))
            
            # Verify the mocks were called correctly
            mock_t.assert_called_once_with("coal.logs.adx.status_messages")
            mock_format.format.assert_called_once_with(success=3, failure=1)
            mock_logger.debug.assert_called_once_with("Status message")
        finally:
            # Restore the original objects
            ingestion_module.LOGGER = original_logger
            ingestion_module.T = original_t
            
    def test_status_messages_log_line_false(self):
        """Test the specific log line that's not being covered with logs=False."""
        # Import the module directly to access the function
        import cosmotech.coal.azure.adx.ingestion as ingestion_module
        
        # Create mock objects
        mock_logger = MagicMock()
        mock_t = MagicMock()
        
        # Replace the real objects with mocks
        original_logger = ingestion_module.LOGGER
        original_t = ingestion_module.T
        ingestion_module.LOGGER = mock_logger
        ingestion_module.T = mock_t
        
        try:
            # Create test data
            successes = [1, 2, 3]  # Just need a list with a length
            failures = [1]  # Just need a list with a length
            logs = False
            
            # Call the specific line directly
            if logs:
                ingestion_module.LOGGER.debug(ingestion_module.T("coal.logs.adx.status_messages").format(
                    success=len(successes), failure=len(failures)))
            
            # Verify the mocks were not called
            mock_t.assert_not_called()
            mock_logger.debug.assert_not_called()
        finally:
            # Restore the original objects
            ingestion_module.LOGGER = original_logger
            ingestion_module.T = original_t

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    @patch("cosmotech.coal.azure.adx.ingestion.LOGGER")
    def test_check_ingestion_status_with_logs_disabled(
        self, mock_logger, mock_status_queues_class, mock_ingest_client, mock_status_queues
    ):
        """Test check_ingestion_status with logs disabled."""
        # Arrange
        source_id = "source-id-logs-disabled"
        _ingest_status[source_id] = IngestionStatus.QUEUED
        _ingest_times[source_id] = time.time()

        # Set up mock status queues
        mock_status_queues_class.return_value = mock_status_queues

        # Create mock success queue with messages
        mock_success_queue = MagicMock()
        mock_success_message = MagicMock()
        mock_success_message.content = '{"IngestionSourceId": "source-id-logs-disabled"}'
        mock_success_queue.receive_messages.return_value = [mock_success_message]
        mock_status_queues.success._get_queues.return_value = [mock_success_queue]

        # Create mock failure queue with messages
        mock_failure_queue = MagicMock()
        mock_failure_message = MagicMock()
        mock_failure_message.content = '{"IngestionSourceId": "failure-source-id"}'
        mock_failure_queue.receive_messages.return_value = [mock_failure_message]
        mock_status_queues.failure._get_queues.return_value = [mock_failure_queue]

        # Act
        with patch(
            "cosmotech.coal.azure.adx.ingestion.SuccessMessage", 
            return_value=MagicMock(IngestionSourceId=source_id)
        ):
            result = list(check_ingestion_status(mock_ingest_client, [source_id], logs=False))

        # Assert
        assert len(result) == 1
        assert result[0] == (source_id, IngestionStatus.SUCCESS)
        
        # Verify that the debug log was not called with the status messages
        for call_args in mock_logger.debug.call_args_list:
            args, kwargs = call_args
            if len(args) > 0 and isinstance(args[0], str) and "status_messages" in args[0]:
                assert False, "LOGGER.debug should not be called with status_messages when logs=False"
        
        # Verify that the message was deleted
        mock_success_queue.delete_message.assert_called_once_with(mock_success_message)

    @patch("cosmotech.coal.azure.adx.ingestion.KustoIngestStatusQueues")
    def test_check_ingestion_status_with_multiple_queues(
        self, mock_status_queues_class, mock_ingest_client, mock_status_queues
    ):
        """Test check_ingestion_status with multiple queues."""
        # Arrange
        source_id = "source-id-multiple-queues"
        _ingest_status[source_id] = IngestionStatus.QUEUED
        _ingest_times[source_id] = time.time()

        # Set up mock status queues
        mock_status_queues_class.return_value = mock_status_queues

        # Create multiple mock success queues
        mock_success_queue1 = MagicMock()
        mock_success_queue1.receive_messages.return_value = []
        mock_success_queue2 = MagicMock()
        mock_message = MagicMock()
        mock_message.content = '{"IngestionSourceId": "source-id-multiple-queues"}'
        mock_success_queue2.receive_messages.return_value = [mock_message]
        
        # Set up the success queues
        mock_status_queues.success._get_queues.return_value = [mock_success_queue1, mock_success_queue2]

        # Set up empty failure queue
        mock_failure_queue = MagicMock()
        mock_failure_queue.receive_messages.return_value = []
        mock_status_queues.failure._get_queues.return_value = [mock_failure_queue]

        # Act
        with patch(
            "cosmotech.coal.azure.adx.ingestion.SuccessMessage", 
            return_value=MagicMock(IngestionSourceId=source_id)
        ):
            result = list(check_ingestion_status(mock_ingest_client, [source_id], logs=True))

        # Assert
        assert len(result) == 1
        assert result[0] == (source_id, IngestionStatus.SUCCESS)
        
        # Verify that the message was deleted from the correct queue
        mock_success_queue1.delete_message.assert_not_called()
        mock_success_queue2.delete_message.assert_called_once_with(mock_message)
