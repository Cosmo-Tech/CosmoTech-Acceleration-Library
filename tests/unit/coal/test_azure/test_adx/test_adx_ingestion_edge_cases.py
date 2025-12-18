# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import time
from unittest.mock import MagicMock, patch

import pytest
from azure.kusto.ingest import QueuedIngestClient
from azure.kusto.ingest.status import KustoIngestStatusQueues
from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.azure.adx.ingestion import (
    IngestionStatus,
    _ingest_status,
    _ingest_times,
    check_ingestion_status,
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
            "cosmotech.coal.azure.adx.ingestion.SuccessMessage", return_value=MagicMock(IngestionSourceId=source_id)
        ):
            result = list(check_ingestion_status(mock_ingest_client, [source_id]))

        # Assert
        assert len(result) == 1
        assert result[0] == (source_id, IngestionStatus.SUCCESS)

        # Verify that the message was deleted from the correct queue
        mock_success_queue1.delete_message.assert_not_called()
        mock_success_queue2.delete_message.assert_called_once_with(mock_message)
