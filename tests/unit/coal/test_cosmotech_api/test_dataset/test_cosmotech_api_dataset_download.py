# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use reproduction translation broadcasting transmission distribution
# etc. to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import patch, MagicMock

import pytest


class TestCosmoTechApiDatasetDownload:
    """Tests for the dataset download module."""

    def test_module_imports(self):
        """Test that the module correctly imports and re-exports functions."""
        # Import the module directly
        import cosmotech.coal.cosmotech_api.dataset.download

        # Verify that the module has the expected attributes
        assert hasattr(cosmotech.coal.cosmotech_api.dataset.download, "download_adt_dataset")
        assert hasattr(cosmotech.coal.cosmotech_api.dataset.download, "download_twingraph_dataset")
        assert hasattr(cosmotech.coal.cosmotech_api.dataset.download, "download_legacy_twingraph_dataset")
        assert hasattr(cosmotech.coal.cosmotech_api.dataset.download, "download_file_dataset")
        assert hasattr(cosmotech.coal.cosmotech_api.dataset.download, "download_dataset_by_id")

        # Verify that the imported functions are callable
        assert callable(cosmotech.coal.cosmotech_api.dataset.download.download_adt_dataset)
        assert callable(cosmotech.coal.cosmotech_api.dataset.download.download_twingraph_dataset)
        assert callable(cosmotech.coal.cosmotech_api.dataset.download.download_legacy_twingraph_dataset)
        assert callable(cosmotech.coal.cosmotech_api.dataset.download.download_file_dataset)
        assert callable(cosmotech.coal.cosmotech_api.dataset.download.download_dataset_by_id)

        # Verify that the functions are imported from the correct modules
        from cosmotech.coal.cosmotech_api.dataset.download.adt import (
            download_adt_dataset as original_download_adt_dataset,
        )
        from cosmotech.coal.cosmotech_api.dataset.download.twingraph import (
            download_twingraph_dataset as original_download_twingraph_dataset,
        )
        from cosmotech.coal.cosmotech_api.dataset.download.twingraph import (
            download_legacy_twingraph_dataset as original_download_legacy_twingraph_dataset,
        )
        from cosmotech.coal.cosmotech_api.dataset.download.file import (
            download_file_dataset as original_download_file_dataset,
        )
        from cosmotech.coal.cosmotech_api.dataset.download.common import (
            download_dataset_by_id as original_download_dataset_by_id,
        )

        assert cosmotech.coal.cosmotech_api.dataset.download.download_adt_dataset is original_download_adt_dataset
        assert (
            cosmotech.coal.cosmotech_api.dataset.download.download_twingraph_dataset
            is original_download_twingraph_dataset
        )
        assert (
            cosmotech.coal.cosmotech_api.dataset.download.download_legacy_twingraph_dataset
            is original_download_legacy_twingraph_dataset
        )
        assert cosmotech.coal.cosmotech_api.dataset.download.download_file_dataset is original_download_file_dataset
        assert cosmotech.coal.cosmotech_api.dataset.download.download_dataset_by_id is original_download_dataset_by_id

    @patch("cosmotech.coal.cosmotech_api.dataset.download.download_adt_dataset")
    def test_download_adt_dataset(self, mock_download_adt_dataset):
        """Test that download_adt_dataset is correctly imported and can be called."""
        # Arrange
        mock_download_adt_dataset.return_value = ("content", "path")

        # Act
        from cosmotech.coal.cosmotech_api.dataset.download import download_adt_dataset

        result = download_adt_dataset("org-123", "dataset-123")

        # Assert
        mock_download_adt_dataset.assert_called_once_with("org-123", "dataset-123")
        assert result == ("content", "path")

    @patch("cosmotech.coal.cosmotech_api.dataset.download.download_twingraph_dataset")
    def test_download_twingraph_dataset(self, mock_download_twingraph_dataset):
        """Test that download_twingraph_dataset is correctly imported and can be called."""
        # Arrange
        mock_download_twingraph_dataset.return_value = ("content", "path")

        # Act
        from cosmotech.coal.cosmotech_api.dataset.download import download_twingraph_dataset

        result = download_twingraph_dataset("org-123", "dataset-123")

        # Assert
        mock_download_twingraph_dataset.assert_called_once_with("org-123", "dataset-123")
        assert result == ("content", "path")

    @patch("cosmotech.coal.cosmotech_api.dataset.download.download_legacy_twingraph_dataset")
    def test_download_legacy_twingraph_dataset(self, mock_download_legacy_twingraph_dataset):
        """Test that download_legacy_twingraph_dataset is correctly imported and can be called."""
        # Arrange
        mock_download_legacy_twingraph_dataset.return_value = ("content", "path")

        # Act
        from cosmotech.coal.cosmotech_api.dataset.download import download_legacy_twingraph_dataset

        result = download_legacy_twingraph_dataset("org-123", "cache-123")

        # Assert
        mock_download_legacy_twingraph_dataset.assert_called_once_with("org-123", "cache-123")
        assert result == ("content", "path")

    @patch("cosmotech.coal.cosmotech_api.dataset.download.download_file_dataset")
    def test_download_file_dataset(self, mock_download_file_dataset):
        """Test that download_file_dataset is correctly imported and can be called."""
        # Arrange
        mock_download_file_dataset.return_value = ("content", "path")

        # Act
        from cosmotech.coal.cosmotech_api.dataset.download import download_file_dataset

        result = download_file_dataset("org-123", "dataset-123")

        # Assert
        mock_download_file_dataset.assert_called_once_with("org-123", "dataset-123")
        assert result == ("content", "path")

    @patch("cosmotech.coal.cosmotech_api.dataset.download.download_dataset_by_id")
    def test_download_dataset_by_id(self, mock_download_dataset_by_id):
        """Test that download_dataset_by_id is correctly imported and can be called."""
        # Arrange
        mock_download_dataset_by_id.return_value = ("content", "path")

        # Act
        from cosmotech.coal.cosmotech_api.dataset.download import download_dataset_by_id

        result = download_dataset_by_id("org-123", "dataset-123")

        # Assert
        mock_download_dataset_by_id.assert_called_once_with("org-123", "dataset-123")
        assert result == ("content", "path")
