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


@pytest.fixture
def base_aws_config():
    return {
        "coal": {"store": "$cosmotech.parameters_absolute_path"},
        "cosmotech": {"dataset_absolute_path": "/path/to/dataset", "parameters_absolute_path": "/path/to/params"},
        "s3": {
            "access_key_id": "test_key",
            "endpoint_url": "http://test.url",
            "secret_access_key": "test_secret",
            "bucket_prefix": "prefix/",
        },
    }


class TestAwsChannel:
    """Tests for the AwsChannel class."""

    def test_init_with_configuration(self, base_aws_config):
        """Test AwsChannel initialization with configuration."""
        # Act
        with patch("cosmotech.coal.store.output.aws_channel.S3"):
            channel = AwsChannel(base_aws_config)

        # Assert
        assert channel.configuration is not None

    def test_required_keys(self):
        """Test that required_keys are properly defined."""
        # Assert
        assert "coal" in AwsChannel.required_keys
        assert "s3" in AwsChannel.required_keys
        assert "store" in AwsChannel.required_keys["coal"]
        assert "access_key_id" in AwsChannel.required_keys["s3"]
        assert "endpoint_url" in AwsChannel.required_keys["s3"]
        assert "secret_access_key" in AwsChannel.required_keys["s3"]

    @patch("cosmotech.coal.store.output.aws_channel.Store")
    @patch("cosmotech.coal.store.output.aws_channel.S3")
    def test_send_sqlite(self, mock_s3_class, mock_store_class, base_aws_config):
        """Test sending data as SQLite database."""
        # Arrange
        mock_s3 = MagicMock()
        mock_s3.output_type = "sqlite"
        mock_s3_class.return_value = mock_s3

        mock_store = MagicMock()
        mock_store._database_path = "/path/to/db.sqlite"
        mock_store_class.return_value = mock_store

        channel = AwsChannel(base_aws_config)

        # Act
        channel.send()

        # Assert
        mock_s3.upload_file.assert_called_once_with("/path/to/db.sqlite", "prefix/db.sqlite")

    @patch("cosmotech.coal.store.output.aws_channel.Store")
    @patch("cosmotech.coal.store.output.aws_channel.S3")
    @patch("cosmotech.coal.store.output.aws_channel.pc.write_csv")
    def test_send_csv(self, mock_write_csv, mock_s3_class, mock_store_class, base_aws_config):
        """Test sending data as CSV files."""
        # Arrange
        mock_s3 = MagicMock()
        mock_s3.output_type = "csv"
        mock_s3_class.return_value = mock_s3

        mock_store = MagicMock()
        mock_table = pa.Table.from_arrays([pa.array([1, 2, 3])], names=["col1"])
        mock_store.list_tables.return_value = ["table1", "table2"]
        mock_store.get_table.return_value = mock_table
        mock_store_class.return_value = mock_store

        channel = AwsChannel(base_aws_config)

        # Act
        channel.send()

        # Assert
        assert mock_s3.upload_data_stream.call_count == 2
        assert mock_write_csv.call_count == 2

    @patch("cosmotech.coal.store.output.aws_channel.Store")
    @patch("cosmotech.coal.store.output.aws_channel.S3")
    @patch("cosmotech.coal.store.output.aws_channel.pq.write_table")
    def test_send_parquet(self, mock_write_parquet, mock_s3_class, mock_store_class, base_aws_config):
        """Test sending data as Parquet files."""
        # Arrange
        mock_s3 = MagicMock()
        mock_s3.output_type = "parquet"
        mock_s3_class.return_value = mock_s3

        mock_store = MagicMock()
        mock_table = pa.Table.from_arrays([pa.array([1, 2, 3])], names=["col1"])
        mock_store.list_tables.return_value = ["table1"]
        mock_store.get_table.return_value = mock_table
        mock_store_class.return_value = mock_store

        channel = AwsChannel(base_aws_config)

        # Act
        channel.send()

        # Assert
        mock_s3.upload_data_stream.assert_called_once()
        mock_write_parquet.assert_called_once()

    @patch("cosmotech.coal.store.output.aws_channel.Store")
    @patch("cosmotech.coal.store.output.aws_channel.S3")
    def test_send_with_tables_filter(self, mock_s3_class, mock_store_class, base_aws_config):
        """Test sending data with table filter."""
        # Arrange
        mock_s3 = MagicMock()
        mock_s3.output_type = "csv"
        mock_s3_class.return_value = mock_s3

        mock_store = MagicMock()
        mock_table = pa.Table.from_arrays([pa.array([1, 2, 3])], names=["col1"])
        mock_store.list_tables.return_value = ["table1", "table2", "table3"]
        mock_store.get_table.return_value = mock_table
        mock_store_class.return_value = mock_store

        channel = AwsChannel(base_aws_config)

        # Act
        channel.send(filter=["table1", "table3"])

        # Assert
        # Should only process table1 and table3
        assert mock_store.get_table.call_count == 2
        mock_store.get_table.assert_any_call("table1")
        mock_store.get_table.assert_any_call("table3")

    @patch("cosmotech.coal.store.output.aws_channel.Store")
    @patch("cosmotech.coal.store.output.aws_channel.S3")
    def test_send_empty_table(self, mock_s3_class, mock_store_class, base_aws_config):
        """Test sending data with empty table."""
        # Arrange
        mock_s3 = MagicMock()
        mock_s3.output_type = "csv"
        mock_s3_class.return_value = mock_s3

        mock_store = MagicMock()
        # Empty table
        mock_table = pa.Table.from_arrays([pa.array([])], names=["col1"])
        mock_store.list_tables.return_value = ["empty_table"]
        mock_store.get_table.return_value = mock_table
        mock_store_class.return_value = mock_store

        channel = AwsChannel(base_aws_config)

        # Act
        channel.send()

        # Assert
        # Should not upload empty table
        mock_s3.upload_data_stream.assert_not_called()

    @patch("cosmotech.coal.store.output.aws_channel.Store")
    @patch("cosmotech.coal.store.output.aws_channel.S3")
    def test_send_invalid_output_type(self, mock_s3_class, mock_store_class, base_aws_config):
        """Test sending data with invalid output type."""
        # Arrange
        mock_s3 = MagicMock()
        mock_s3.output_type = "invalid_type"
        mock_s3_class.return_value = mock_s3

        mock_store = MagicMock()
        mock_store_class.return_value = mock_store

        channel = AwsChannel(base_aws_config)

        # Act & Assert
        with pytest.raises(ValueError):
            channel.send()

    @patch("cosmotech.coal.store.output.aws_channel.S3")
    def test_delete(self, mock_s3_class, base_aws_config):
        """Test delete method."""
        # Arrange
        mock_s3 = MagicMock()
        mock_s3_class.return_value = mock_s3

        channel = AwsChannel(base_aws_config)

        # Act
        channel.delete()

        # Assert
        mock_s3.delete_objects.assert_called_once()
