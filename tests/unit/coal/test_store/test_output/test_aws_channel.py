# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from cosmotech.coal.store.output.aws_channel import AwsChannel


class TestAwsChannel:
    """Tests for AwsChannel class."""

    @pytest.fixture
    def mock_configuration(self):
        """Create a mock Configuration."""
        mock_config = MagicMock()
        mock_config.parameters_absolute_path = "/test/path"
        mock_config.s3.bucket_prefix = "test-prefix/"
        return mock_config

    @pytest.fixture
    def mock_s3(self):
        """Create a mock S3 client."""
        mock = MagicMock()
        mock.output_type = "csv"
        return mock

    @pytest.fixture
    def mock_store(self):
        """Create a mock Store."""
        mock = MagicMock()
        mock._database_path = "/test/db.sqlite"
        mock.list_tables.return_value = ["table1", "table2"]

        # Create sample table data
        sample_data = pa.table({"id": ["1", "2", "3"], "value": [10, 20, 30]})
        mock.get_table.return_value = sample_data
        return mock

    @patch("cosmotech.coal.store.output.aws_channel.Configuration")
    @patch("cosmotech.coal.store.output.aws_channel.S3")
    def test_init(self, mock_s3_class, mock_config_class):
        """Test AwsChannel initialization."""
        # Arrange
        mock_config = MagicMock()
        mock_config_class.return_value = mock_config
        mock_s3 = MagicMock()
        mock_s3_class.return_value = mock_s3

        # Act
        channel = AwsChannel()

        # Assert
        assert channel.configuration == mock_config
        assert channel._s3 == mock_s3
        mock_config_class.assert_called_once()
        mock_s3_class.assert_called_once_with(mock_config)

    @patch("cosmotech.coal.store.output.aws_channel.Configuration")
    @patch("cosmotech.coal.store.output.aws_channel.S3")
    @patch("cosmotech.coal.store.output.aws_channel.Store")
    def test_send_sqlite(self, mock_store_class, mock_s3_class, mock_config_class, mock_store):
        """Test send method with sqlite output type."""
        # Arrange
        mock_config = MagicMock()
        mock_config.parameters_absolute_path = "/test/path"
        mock_config.s3.bucket_prefix = "test-prefix/"
        mock_config_class.return_value = mock_config

        mock_s3 = MagicMock()
        mock_s3.output_type = "sqlite"
        mock_s3_class.return_value = mock_s3

        mock_store._database_path = "/test/db.sqlite"
        mock_store_class.return_value = mock_store

        channel = AwsChannel()

        # Act
        channel.send()

        # Assert
        mock_s3.upload_file.assert_called_once_with("/test/db.sqlite", "test-prefix/db.sqlite")

    @patch("cosmotech.coal.store.output.aws_channel.Configuration")
    @patch("cosmotech.coal.store.output.aws_channel.S3")
    @patch("cosmotech.coal.store.output.aws_channel.Store")
    def test_send_csv(self, mock_store_class, mock_s3_class, mock_config_class, mock_store):
        """Test send method with csv output type."""
        # Arrange
        mock_config = MagicMock()
        mock_config.parameters_absolute_path = "/test/path"
        mock_config_class.return_value = mock_config

        mock_s3 = MagicMock()
        mock_s3.output_type = "csv"
        mock_s3_class.return_value = mock_s3

        mock_store_class.return_value = mock_store

        channel = AwsChannel()

        # Act
        channel.send()

        # Assert
        assert mock_s3.upload_data_stream.call_count == 2  # Two tables
        # Check that CSV files were created
        calls = mock_s3.upload_data_stream.call_args_list
        assert "table1.csv" in str(calls[0])
        assert "table2.csv" in str(calls[1])

    @patch("cosmotech.coal.store.output.aws_channel.Configuration")
    @patch("cosmotech.coal.store.output.aws_channel.S3")
    @patch("cosmotech.coal.store.output.aws_channel.Store")
    def test_send_parquet(self, mock_store_class, mock_s3_class, mock_config_class, mock_store):
        """Test send method with parquet output type."""
        # Arrange
        mock_config = MagicMock()
        mock_config.parameters_absolute_path = "/test/path"
        mock_config_class.return_value = mock_config

        mock_s3 = MagicMock()
        mock_s3.output_type = "parquet"
        mock_s3_class.return_value = mock_s3

        mock_store_class.return_value = mock_store

        channel = AwsChannel()

        # Act
        channel.send()

        # Assert
        assert mock_s3.upload_data_stream.call_count == 2  # Two tables
        # Check that Parquet files were created
        calls = mock_s3.upload_data_stream.call_args_list
        assert "table1.parquet" in str(calls[0])
        assert "table2.parquet" in str(calls[1])

    @patch("cosmotech.coal.store.output.aws_channel.Configuration")
    @patch("cosmotech.coal.store.output.aws_channel.S3")
    @patch("cosmotech.coal.store.output.aws_channel.Store")
    def test_send_with_filter(self, mock_store_class, mock_s3_class, mock_config_class, mock_store):
        """Test send method with tables filter."""
        # Arrange
        mock_config = MagicMock()
        mock_config.parameters_absolute_path = "/test/path"
        mock_config_class.return_value = mock_config

        mock_s3 = MagicMock()
        mock_s3.output_type = "csv"
        mock_s3_class.return_value = mock_s3

        mock_store_class.return_value = mock_store

        channel = AwsChannel()

        # Act
        channel.send(tables_filter=["table1"])

        # Assert
        assert mock_s3.upload_data_stream.call_count == 1  # Only table1
        calls = mock_s3.upload_data_stream.call_args_list
        assert "table1.csv" in str(calls[0])

    @patch("cosmotech.coal.store.output.aws_channel.Configuration")
    @patch("cosmotech.coal.store.output.aws_channel.S3")
    @patch("cosmotech.coal.store.output.aws_channel.Store")
    def test_send_empty_table_skipped(self, mock_store_class, mock_s3_class, mock_config_class):
        """Test send method skips empty tables."""
        # Arrange
        mock_config = MagicMock()
        mock_config.parameters_absolute_path = "/test/path"
        mock_config_class.return_value = mock_config

        mock_s3 = MagicMock()
        mock_s3.output_type = "csv"
        mock_s3_class.return_value = mock_s3

        mock_store = MagicMock()
        mock_store.list_tables.return_value = ["empty_table"]
        # Create empty table
        empty_table = pa.table({"id": []})
        mock_store.get_table.return_value = empty_table
        mock_store_class.return_value = mock_store

        channel = AwsChannel()

        # Act
        channel.send()

        # Assert
        mock_s3.upload_data_stream.assert_not_called()

    @patch("cosmotech.coal.store.output.aws_channel.Configuration")
    @patch("cosmotech.coal.store.output.aws_channel.S3")
    @patch("cosmotech.coal.store.output.aws_channel.Store")
    def test_send_invalid_output_type(self, mock_store_class, mock_s3_class, mock_config_class):
        """Test send method raises error for invalid output type."""
        # Arrange
        mock_config = MagicMock()
        mock_config.parameters_absolute_path = "/test/path"
        mock_config_class.return_value = mock_config

        mock_s3 = MagicMock()
        mock_s3.output_type = "invalid"
        mock_s3_class.return_value = mock_s3

        mock_store = MagicMock()
        mock_store_class.return_value = mock_store

        channel = AwsChannel()

        # Act & Assert
        with pytest.raises(ValueError):
            channel.send()
