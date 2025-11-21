# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib
from io import BytesIO
from unittest.mock import MagicMock, call, patch

import pytest

from cosmotech.coal.aws import S3
from cosmotech.coal.utils.configuration import Configuration


@pytest.fixture
def base_configuration():
    _c = Configuration()
    _c.s3.use_ssl = True
    _c.s3.endpoint_url = "https://s3.example.com"
    _c.s3.access_key_id = "test-access-id"
    _c.s3.secret_access_key = "test-secret-key"
    _c.s3.bucket_name = "test-bucket"
    _c.s3.bucket_prefix = "prefix/"
    return _c


@pytest.fixture
def no_prefix_configuration(base_configuration):
    del base_configuration.s3.bucket_prefix
    return base_configuration


class TestS3Functions:
    """Tests for top-level functions in the s3 module."""

    @patch("boto3.client")
    def test_create_s3_client(self, mock_boto3_client, base_configuration):
        """Test the create_s3_client function."""
        # Arrange
        _s3 = S3(base_configuration)

        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        # Act
        result = _s3.client

        # Assert
        mock_boto3_client.assert_called_once_with(
            "s3",
            use_ssl=base_configuration.s3.use_ssl,
            endpoint_url=base_configuration.s3.endpoint_url,
            aws_access_key_id=base_configuration.s3.access_key_id,
            aws_secret_access_key=base_configuration.s3.secret_access_key,
        )
        assert result == mock_client

    @patch("boto3.client")
    def test_create_s3_client_with_ssl_cert(self, mock_boto3_client, base_configuration):
        """Test the create_s3_client function with SSL certificate."""
        # Arrange
        base_configuration.s3.ssl_cert_bundle = "/path/to/cert.pem"
        _s3 = S3(base_configuration)

        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        # Act
        result = _s3.client

        # Assert
        mock_boto3_client.assert_called_once_with(
            "s3",
            use_ssl=base_configuration.s3.use_ssl,
            endpoint_url=base_configuration.s3.endpoint_url,
            aws_access_key_id=base_configuration.s3.access_key_id,
            aws_secret_access_key=base_configuration.s3.secret_access_key,
            verify=base_configuration.s3.ssl_cert_bundle,
        )
        assert result == mock_client

    @patch("boto3.resource")
    def test_create_s3_resource(self, mock_boto3_resource, base_configuration):
        """Test the create_s3_resource function."""
        # Arrange
        _s3 = S3(base_configuration)

        mock_resource = MagicMock()
        mock_boto3_resource.return_value = mock_resource

        # Act
        result = _s3.resource

        # Assert
        mock_boto3_resource.assert_called_once_with(
            "s3",
            use_ssl=base_configuration.s3.use_ssl,
            endpoint_url=base_configuration.s3.endpoint_url,
            aws_access_key_id=base_configuration.s3.access_key_id,
            aws_secret_access_key=base_configuration.s3.secret_access_key,
        )
        assert result == mock_resource

    @patch("boto3.resource")
    def test_create_s3_resource_with_ssl_cert(self, mock_boto3_resource, base_configuration):
        """Test the create_s3_resource function with SSL certificate."""
        # Arrange
        base_configuration.s3.ssl_cert_bundle = "/path/to/cert.pem"
        _s3 = S3(base_configuration)

        mock_resource = MagicMock()
        mock_boto3_resource.return_value = mock_resource

        # Act
        result = _s3.resource

        # Assert
        mock_boto3_resource.assert_called_once_with(
            "s3",
            use_ssl=base_configuration.s3.use_ssl,
            endpoint_url=base_configuration.s3.endpoint_url,
            aws_access_key_id=base_configuration.s3.access_key_id,
            aws_secret_access_key=base_configuration.s3.secret_access_key,
            verify=base_configuration.s3.ssl_cert_bundle,
        )
        assert result == mock_resource

    @patch("boto3.resource")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_file(self, mock_logger, mock_boto3_resource, base_configuration):
        """Test the upload_file function."""
        # Arrange
        file_path = pathlib.Path("/path/to/file.txt")
        _s3 = S3(base_configuration)

        mock_resource = MagicMock()
        mock_boto3_resource.return_value = mock_resource
        mock_bucket = MagicMock()
        mock_resource.Bucket.return_value = mock_bucket

        # Act
        _s3.upload_file(file_path)

        # Assert
        mock_resource.Bucket.assert_called_once_with(base_configuration.s3.bucket_name)
        mock_bucket.upload_file.assert_called_once_with(str(file_path), "prefix/file.txt")
        mock_logger.info.assert_called_once()

    @patch("boto3.resource")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_file_no_prefix(self, mock_logger, mock_boto3_resource, no_prefix_configuration):
        """Test the upload_file function without a prefix."""
        # Arrange
        file_path = pathlib.Path("/path/to/file.txt")
        _s3 = S3(no_prefix_configuration)

        mock_resource = MagicMock()
        mock_boto3_resource.return_value = mock_resource
        mock_bucket = MagicMock()
        mock_resource.Bucket.return_value = mock_bucket

        # Act
        _s3.upload_file(file_path)

        # Assert
        mock_resource.Bucket.assert_called_once_with(no_prefix_configuration.s3.bucket_name)
        mock_bucket.upload_file.assert_called_once_with(str(file_path), "file.txt")
        mock_logger.info.assert_called_once()

    @patch("boto3.resource")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.is_dir")
    @patch("pathlib.Path.glob")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_folder(
        self, mock_logger, mock_glob, mock_is_dir, mock_exists, mock_boto3_resource, base_configuration
    ):
        """Test the upload_folder function."""
        # Arrange
        source_folder = "/path/to/folder"
        recursive = False
        _s3 = S3(base_configuration)

        mock_resource = MagicMock()
        mock_boto3_resource.return_value = mock_resource
        mock_bucket = MagicMock()
        mock_resource.Bucket.return_value = mock_bucket

        # Mock Path.exists and Path.is_dir
        mock_exists.return_value = True
        mock_is_dir.return_value = True

        # Mock Path.glob to return a list of files
        file1 = MagicMock()
        file1.is_file.return_value = True
        file1.name = "file1.txt"
        file1.__str__.return_value = "/path/to/folder/file1.txt"

        file2 = MagicMock()
        file2.is_file.return_value = True
        file2.name = "file2.txt"
        file2.__str__.return_value = "/path/to/folder/file2.txt"

        mock_glob.return_value = [file1, file2]

        # Act
        _s3.upload_folder(source_folder, recursive)

        # Assert
        mock_exists.assert_called_once()
        mock_is_dir.assert_called_once()
        mock_glob.assert_called_once_with("*")  # Non-recursive glob
        mock_resource.Bucket.assert_called_with(base_configuration.s3.bucket_name)
        assert mock_bucket.upload_file.call_count == 2
        mock_bucket.upload_file.assert_has_calls(
            [
                call("/path/to/folder/file1.txt", "prefix/file1.txt"),
                call("/path/to/folder/file2.txt", "prefix/file2.txt"),
            ]
        )
        assert mock_logger.info.call_count == 2

    @patch("boto3.resource")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.is_dir")
    @patch("pathlib.Path.glob")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_folder_recursive(
        self, mock_logger, mock_glob, mock_is_dir, mock_exists, mock_boto3_resource, base_configuration
    ):
        """Test the upload_folder function with recursive option."""
        # Arrange
        source_folder = "/path/to/folder"
        recursive = True
        _s3 = S3(base_configuration)

        mock_resource = MagicMock()
        mock_boto3_resource.return_value = mock_resource
        mock_bucket = MagicMock()
        mock_resource.Bucket.return_value = mock_bucket

        # Mock Path.exists and Path.is_dir
        mock_exists.return_value = True
        mock_is_dir.return_value = True

        # Mock Path.glob to return a list of files including subdirectory
        file1 = MagicMock()
        file1.is_file.return_value = True
        file1.__str__.return_value = "/path/to/folder/file1.txt"

        file2 = MagicMock()
        file2.is_file.return_value = True
        file2.__str__.return_value = "/path/to/folder/subdir/file2.txt"

        mock_glob.return_value = [file1, file2]

        # Act
        _s3.upload_folder(source_folder, recursive)

        # Assert
        mock_exists.assert_called_once()
        mock_is_dir.assert_called_once()
        mock_glob.assert_called_once_with("**/*")  # Recursive glob
        mock_resource.Bucket.assert_called_with(base_configuration.s3.bucket_name)
        assert mock_bucket.upload_file.call_count == 2
        mock_bucket.upload_file.assert_has_calls(
            [
                call("/path/to/folder/file1.txt", "prefix/file1.txt"),
                call("/path/to/folder/subdir/file2.txt", "prefix/subdir/file2.txt"),
            ]
        )
        assert mock_logger.info.call_count == 2

    @patch("boto3.resource")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.is_dir")
    @patch("cosmotech.coal.aws.s3.S3.upload_file")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_folder_single_file(
        self, mock_logger, mock_upload_file, mock_is_dir, mock_exists, mock_boto3_resource, base_configuration
    ):
        """Test the upload_folder function with a file instead of a directory."""
        # Arrange
        source_folder = "/path/to/file.txt"
        recursive = False
        _s3 = S3(base_configuration)

        # Mock Path.exists and Path.is_dir
        mock_exists.return_value = True
        mock_is_dir.return_value = False

        # Act
        _s3.upload_folder(source_folder, recursive)

        # Assert
        mock_exists.assert_called_once()
        mock_is_dir.assert_called_once()
        mock_upload_file.assert_called_once_with(pathlib.Path(source_folder))

    @patch("pathlib.Path.exists")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_folder_not_found(self, mock_logger, mock_exists, base_configuration):
        """Test the upload_folder function with a non-existent folder."""
        # Arrange
        source_folder = "/path/to/nonexistent"
        _s3 = S3(base_configuration)

        # Mock Path.exists to return False
        mock_exists.return_value = False

        # Act & Assert
        with pytest.raises(FileNotFoundError):
            _s3.upload_folder(source_folder)

        mock_exists.assert_called_once()
        mock_logger.error.assert_called_once()

    @patch("boto3.resource")
    @patch("pathlib.Path.mkdir")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_download_files(self, mock_logger, mock_mkdir, mock_boto3_resource, base_configuration):
        """Test the download_files function."""
        # Arrange
        destination_folder = "/path/to/target"
        _s3 = S3(base_configuration)

        mock_resource = MagicMock()
        mock_boto3_resource.return_value = mock_resource
        mock_bucket = MagicMock()
        mock_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.filter to return a list of objects
        file1 = MagicMock()
        file1.key = "prefix/file1.txt"

        file2 = MagicMock()
        file2.key = "prefix/subdir/file2.txt"

        mock_bucket.objects.filter.return_value = [file1, file2]

        # Act
        _s3.download_files(destination_folder)

        # Assert
        mock_resource.Bucket.assert_called_once_with(base_configuration.s3.bucket_name)
        mock_bucket.objects.filter.assert_called_once_with(Prefix=base_configuration.s3.bucket_prefix)
        mock_mkdir.assert_called()
        assert mock_bucket.download_file.call_count == 2
        mock_bucket.download_file.assert_has_calls(
            [
                call("prefix/file1.txt", "/path/to/target/file1.txt"),
                call("prefix/subdir/file2.txt", "/path/to/target/subdir/file2.txt"),
            ]
        )
        assert mock_logger.info.call_count == 2

    @patch("boto3.resource")
    @patch("pathlib.Path.mkdir")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_download_files_no_prefix(self, mock_logger, mock_mkdir, mock_boto3_resource, no_prefix_configuration):
        """Test the download_files function without a prefix."""
        # Arrange
        target_folder = "/path/to/target"
        _s3 = S3(no_prefix_configuration)

        mock_resource = MagicMock()
        mock_boto3_resource.return_value = mock_resource
        mock_bucket = MagicMock()
        mock_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.all to return a list of objects
        file1 = MagicMock()
        file1.key = "file1.txt"

        file2 = MagicMock()
        file2.key = "subdir/file2.txt"

        mock_bucket.objects.all.return_value = [file1, file2]

        # Act
        _s3.download_files(target_folder)

        # Assert
        mock_resource.Bucket.assert_called_once_with(no_prefix_configuration.s3.bucket_name)
        mock_bucket.objects.all.assert_called_once()
        mock_mkdir.assert_called()
        assert mock_bucket.download_file.call_count == 2
        mock_bucket.download_file.assert_has_calls(
            [
                call("file1.txt", "/path/to/target/file1.txt"),
                call("subdir/file2.txt", "/path/to/target/subdir/file2.txt"),
            ]
        )
        assert mock_logger.info.call_count == 2

    @patch("boto3.resource")
    @patch("pathlib.Path.mkdir")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_download_files_skip_directories(
        self, mock_logger, mock_mkdir, mock_boto3_resource, no_prefix_configuration
    ):
        """Test the download_files function skips directory objects."""
        # Arrange
        target_folder = "/path/to/target"
        _s3 = S3(no_prefix_configuration)

        mock_resource = MagicMock()
        mock_boto3_resource.return_value = mock_resource
        mock_bucket = MagicMock()
        mock_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.all to return a list of objects including a directory
        file1 = MagicMock()
        file1.key = "file1.txt"

        directory = MagicMock()
        directory.key = "subdir/"  # Directory ends with /

        mock_bucket.objects.all.return_value = [file1, directory]

        # Act
        _s3.download_files(target_folder)

        # Assert
        mock_resource.Bucket.assert_called_once_with(no_prefix_configuration.s3.bucket_name)
        mock_bucket.objects.all.assert_called_once()
        mock_mkdir.assert_called()
        # Only the file should be downloaded, not the directory
        mock_bucket.download_file.assert_called_once_with("file1.txt", "/path/to/target/file1.txt")
        assert mock_logger.info.call_count == 1

    @patch("boto3.client")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_data_stream(self, mock_logger, mock_boto3_client, base_configuration):
        """Test the upload_data_stream function."""
        # Arrange
        data_stream = BytesIO(b"test data")
        file_name = "file.txt"
        _s3 = S3(base_configuration)

        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        # Act
        _s3.upload_data_stream(data_stream, file_name)

        # Assert
        mock_s3_client.upload_fileobj.assert_called_once_with(
            data_stream, base_configuration.s3.bucket_name, "prefix/file.txt"
        )
        mock_logger.info.assert_called_once()

    @patch("boto3.client")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_data_stream_no_prefix(self, mock_logger, mock_boto3_client, no_prefix_configuration):
        """Test the upload_data_stream function without a prefix."""
        # Arrange
        data_stream = BytesIO(b"test data")
        file_name = "file.txt"
        _s3 = S3(no_prefix_configuration)

        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        # Act
        _s3.upload_data_stream(data_stream, file_name)

        # Assert
        mock_s3_client.upload_fileobj.assert_called_once_with(
            data_stream, no_prefix_configuration.s3.bucket_name, "file.txt"
        )
        mock_logger.info.assert_called_once()

    @patch("boto3.resource")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_delete_objects(self, mock_logger, mock_boto3_resource, base_configuration):
        """Test the delete_objects function."""
        # Arrange
        _s3 = S3(base_configuration)

        mock_s3_resource = MagicMock()
        mock_boto3_resource.return_value = mock_s3_resource
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.filter to return a list of objects
        file1 = MagicMock()
        file1.key = "prefix/file1.txt"

        file2 = MagicMock()
        file2.key = "prefix/file2.txt"

        mock_bucket.objects.filter.return_value = [file1, file2]

        # Act
        _s3.delete_objects()

        # Assert
        mock_s3_resource.Bucket.assert_called_once_with(base_configuration.s3.bucket_name)
        mock_bucket.objects.filter.assert_called_once_with(Prefix=base_configuration.s3.bucket_prefix)
        mock_bucket.delete_objects.assert_called_once_with(
            Delete={"Objects": [{"Key": "prefix/file1.txt"}, {"Key": "prefix/file2.txt"}]}
        )
        mock_logger.info.assert_called_once()

    @patch("boto3.resource")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_delete_objects_no_prefix(self, mock_logger, mock_boto3_resource, no_prefix_configuration):
        """Test the delete_objects function without a prefix."""
        # Arrange
        _s3 = S3(no_prefix_configuration)

        mock_s3_resource = MagicMock()
        mock_boto3_resource.return_value = mock_s3_resource
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.all to return a list of objects
        file1 = MagicMock()
        file1.key = "file1.txt"

        file2 = MagicMock()
        file2.key = "file2.txt"

        mock_bucket.objects.all.return_value = [file1, file2]

        # Act
        _s3.delete_objects()

        # Assert
        mock_s3_resource.Bucket.assert_called_once_with(no_prefix_configuration.s3.bucket_name)
        mock_bucket.objects.all.assert_called_once()
        mock_bucket.delete_objects.assert_called_once_with(
            Delete={"Objects": [{"Key": "file1.txt"}, {"Key": "file2.txt"}]}
        )
        mock_logger.info.assert_called_once()

    @patch("boto3.resource")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_delete_objects_empty(self, mock_logger, mock_boto3_resource, no_prefix_configuration):
        """Test the delete_objects function with no objects to delete."""
        # Arrange
        _s3 = S3(no_prefix_configuration)

        mock_s3_resource = MagicMock()
        mock_boto3_resource.return_value = mock_s3_resource
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.all to return an empty list
        mock_bucket.objects.all.return_value = []

        # Act
        _s3.delete_objects()

        # Assert
        mock_s3_resource.Bucket.assert_called_once_with(no_prefix_configuration.s3.bucket_name)
        mock_bucket.objects.all.assert_called_once()
        mock_bucket.delete_objects.assert_not_called()
        mock_logger.info.assert_called_once()

    @patch("boto3.resource")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_delete_objects_skip_prefix(self, mock_logger, mock_boto3_resource, base_configuration):
        """Test the delete_objects function skips the prefix itself."""
        # Arrange
        _s3 = S3(base_configuration)

        mock_s3_resource = MagicMock()
        mock_boto3_resource.return_value = mock_s3_resource
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.filter to return a list including the prefix itself
        prefix_obj = MagicMock()
        prefix_obj.key = "prefix/"

        file1 = MagicMock()
        file1.key = "prefix/file1.txt"

        mock_bucket.objects.filter.return_value = [prefix_obj, file1]

        # Act
        _s3.delete_objects()

        # Assert
        mock_s3_resource.Bucket.assert_called_once_with(base_configuration.s3.bucket_name)
        mock_bucket.objects.filter.assert_called_once_with(Prefix=base_configuration.s3.bucket_prefix)
        # Only file1 should be deleted, not the prefix itself
        mock_bucket.delete_objects.assert_called_once_with(Delete={"Objects": [{"Key": "prefix/file1.txt"}]})
        mock_logger.info.assert_called_once()
