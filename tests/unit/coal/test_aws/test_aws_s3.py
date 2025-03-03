# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib
from io import BytesIO

import pytest
from unittest.mock import MagicMock, patch, call

from cosmotech.coal.aws.s3 import (
    create_s3_client,
    create_s3_resource,
    upload_file,
    upload_folder,
    download_files,
    upload_data_stream,
    delete_objects,
)


class TestS3Functions:
    """Tests for top-level functions in the s3 module."""

    @patch("boto3.client")
    def test_create_s3_client(self, mock_boto3_client):
        """Test the create_s3_client function."""
        # Arrange
        endpoint_url = "https://s3.example.com"
        access_id = "test-access-id"
        secret_key = "test-secret-key"
        use_ssl = True
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        # Act
        result = create_s3_client(endpoint_url, access_id, secret_key, use_ssl)

        # Assert
        mock_boto3_client.assert_called_once_with(
            "s3",
            use_ssl=use_ssl,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_id,
            aws_secret_access_key=secret_key,
        )
        assert result == mock_client

    @patch("boto3.client")
    def test_create_s3_client_with_ssl_cert(self, mock_boto3_client):
        """Test the create_s3_client function with SSL certificate."""
        # Arrange
        endpoint_url = "https://s3.example.com"
        access_id = "test-access-id"
        secret_key = "test-secret-key"
        use_ssl = True
        ssl_cert_bundle = "/path/to/cert.pem"
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        # Act
        result = create_s3_client(endpoint_url, access_id, secret_key, use_ssl, ssl_cert_bundle)

        # Assert
        mock_boto3_client.assert_called_once_with(
            "s3",
            use_ssl=use_ssl,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_id,
            aws_secret_access_key=secret_key,
            verify=ssl_cert_bundle,
        )
        assert result == mock_client

    @patch("boto3.resource")
    def test_create_s3_resource(self, mock_boto3_resource):
        """Test the create_s3_resource function."""
        # Arrange
        endpoint_url = "https://s3.example.com"
        access_id = "test-access-id"
        secret_key = "test-secret-key"
        use_ssl = True
        mock_resource = MagicMock()
        mock_boto3_resource.return_value = mock_resource

        # Act
        result = create_s3_resource(endpoint_url, access_id, secret_key, use_ssl)

        # Assert
        mock_boto3_resource.assert_called_once_with(
            "s3",
            use_ssl=use_ssl,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_id,
            aws_secret_access_key=secret_key,
        )
        assert result == mock_resource

    @patch("boto3.resource")
    def test_create_s3_resource_with_ssl_cert(self, mock_boto3_resource):
        """Test the create_s3_resource function with SSL certificate."""
        # Arrange
        endpoint_url = "https://s3.example.com"
        access_id = "test-access-id"
        secret_key = "test-secret-key"
        use_ssl = True
        ssl_cert_bundle = "/path/to/cert.pem"
        mock_resource = MagicMock()
        mock_boto3_resource.return_value = mock_resource

        # Act
        result = create_s3_resource(endpoint_url, access_id, secret_key, use_ssl, ssl_cert_bundle)

        # Assert
        mock_boto3_resource.assert_called_once_with(
            "s3",
            use_ssl=use_ssl,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_id,
            aws_secret_access_key=secret_key,
            verify=ssl_cert_bundle,
        )
        assert result == mock_resource

    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_file(self, mock_logger):
        """Test the upload_file function."""
        # Arrange
        file_path = pathlib.Path("/path/to/file.txt")
        bucket_name = "test-bucket"
        file_prefix = "prefix/"
        mock_s3_resource = MagicMock()
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Act
        upload_file(file_path, bucket_name, mock_s3_resource, file_prefix)

        # Assert
        mock_s3_resource.Bucket.assert_called_once_with(bucket_name)
        mock_bucket.upload_file.assert_called_once_with(str(file_path), "prefix/file.txt")
        mock_logger.info.assert_called_once()

    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_file_no_prefix(self, mock_logger):
        """Test the upload_file function without a prefix."""
        # Arrange
        file_path = pathlib.Path("/path/to/file.txt")
        bucket_name = "test-bucket"
        mock_s3_resource = MagicMock()
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Act
        upload_file(file_path, bucket_name, mock_s3_resource)

        # Assert
        mock_s3_resource.Bucket.assert_called_once_with(bucket_name)
        mock_bucket.upload_file.assert_called_once_with(str(file_path), "file.txt")
        mock_logger.info.assert_called_once()

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.is_dir")
    @patch("pathlib.Path.glob")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_folder(self, mock_logger, mock_glob, mock_is_dir, mock_exists):
        """Test the upload_folder function."""
        # Arrange
        source_folder = "/path/to/folder"
        bucket_name = "test-bucket"
        file_prefix = "prefix/"
        recursive = False
        mock_s3_resource = MagicMock()
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

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
        upload_folder(source_folder, bucket_name, mock_s3_resource, file_prefix, recursive)

        # Assert
        mock_exists.assert_called_once()
        mock_is_dir.assert_called_once()
        mock_glob.assert_called_once_with("*")  # Non-recursive glob
        mock_s3_resource.Bucket.assert_called_with(bucket_name)
        assert mock_bucket.upload_file.call_count == 2
        mock_bucket.upload_file.assert_has_calls(
            [
                call("/path/to/folder/file1.txt", "prefix/file1.txt"),
                call("/path/to/folder/file2.txt", "prefix/file2.txt"),
            ]
        )
        assert mock_logger.info.call_count == 2

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.is_dir")
    @patch("pathlib.Path.glob")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_folder_recursive(self, mock_logger, mock_glob, mock_is_dir, mock_exists):
        """Test the upload_folder function with recursive option."""
        # Arrange
        source_folder = "/path/to/folder"
        bucket_name = "test-bucket"
        file_prefix = "prefix/"
        recursive = True
        mock_s3_resource = MagicMock()
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

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
        upload_folder(source_folder, bucket_name, mock_s3_resource, file_prefix, recursive)

        # Assert
        mock_exists.assert_called_once()
        mock_is_dir.assert_called_once()
        mock_glob.assert_called_once_with("**/*")  # Recursive glob
        mock_s3_resource.Bucket.assert_called_with(bucket_name)
        assert mock_bucket.upload_file.call_count == 2
        mock_bucket.upload_file.assert_has_calls(
            [
                call("/path/to/folder/file1.txt", "prefix/file1.txt"),
                call("/path/to/folder/subdir/file2.txt", "prefix/subdir/file2.txt"),
            ]
        )
        assert mock_logger.info.call_count == 2

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.is_dir")
    @patch("cosmotech.coal.aws.s3.upload_file")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_folder_single_file(self, mock_logger, mock_upload_file, mock_is_dir, mock_exists):
        """Test the upload_folder function with a file instead of a directory."""
        # Arrange
        source_folder = "/path/to/file.txt"
        bucket_name = "test-bucket"
        file_prefix = "prefix/"
        recursive = False
        mock_s3_resource = MagicMock()

        # Mock Path.exists and Path.is_dir
        mock_exists.return_value = True
        mock_is_dir.return_value = False

        # Act
        upload_folder(source_folder, bucket_name, mock_s3_resource, file_prefix, recursive)

        # Assert
        mock_exists.assert_called_once()
        mock_is_dir.assert_called_once()
        mock_upload_file.assert_called_once_with(
            pathlib.Path(source_folder), bucket_name, mock_s3_resource, file_prefix
        )

    @patch("pathlib.Path.exists")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_folder_not_found(self, mock_logger, mock_exists):
        """Test the upload_folder function with a non-existent folder."""
        # Arrange
        source_folder = "/path/to/nonexistent"
        bucket_name = "test-bucket"
        mock_s3_resource = MagicMock()

        # Mock Path.exists to return False
        mock_exists.return_value = False

        # Act & Assert
        with pytest.raises(FileNotFoundError):
            upload_folder(source_folder, bucket_name, mock_s3_resource)

        mock_exists.assert_called_once()
        mock_logger.error.assert_called_once()

    @patch("pathlib.Path.mkdir")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_download_files(self, mock_logger, mock_mkdir):
        """Test the download_files function."""
        # Arrange
        target_folder = "/path/to/target"
        bucket_name = "test-bucket"
        file_prefix = "prefix/"
        mock_s3_resource = MagicMock()
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.filter to return a list of objects
        file1 = MagicMock()
        file1.key = "prefix/file1.txt"

        file2 = MagicMock()
        file2.key = "prefix/subdir/file2.txt"

        mock_bucket.objects.filter.return_value = [file1, file2]

        # Act
        download_files(target_folder, bucket_name, mock_s3_resource, file_prefix)

        # Assert
        mock_s3_resource.Bucket.assert_called_once_with(bucket_name)
        mock_bucket.objects.filter.assert_called_once_with(Prefix=file_prefix)
        mock_mkdir.assert_called()
        assert mock_bucket.download_file.call_count == 2
        mock_bucket.download_file.assert_has_calls(
            [
                call("prefix/file1.txt", "/path/to/target/file1.txt"),
                call("prefix/subdir/file2.txt", "/path/to/target/subdir/file2.txt"),
            ]
        )
        assert mock_logger.info.call_count == 2

    @patch("pathlib.Path.mkdir")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_download_files_no_prefix(self, mock_logger, mock_mkdir):
        """Test the download_files function without a prefix."""
        # Arrange
        target_folder = "/path/to/target"
        bucket_name = "test-bucket"
        mock_s3_resource = MagicMock()
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.all to return a list of objects
        file1 = MagicMock()
        file1.key = "file1.txt"

        file2 = MagicMock()
        file2.key = "subdir/file2.txt"

        mock_bucket.objects.all.return_value = [file1, file2]

        # Act
        download_files(target_folder, bucket_name, mock_s3_resource)

        # Assert
        mock_s3_resource.Bucket.assert_called_once_with(bucket_name)
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

    @patch("pathlib.Path.mkdir")
    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_download_files_skip_directories(self, mock_logger, mock_mkdir):
        """Test the download_files function skips directory objects."""
        # Arrange
        target_folder = "/path/to/target"
        bucket_name = "test-bucket"
        mock_s3_resource = MagicMock()
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.all to return a list of objects including a directory
        file1 = MagicMock()
        file1.key = "file1.txt"

        directory = MagicMock()
        directory.key = "subdir/"  # Directory ends with /

        mock_bucket.objects.all.return_value = [file1, directory]

        # Act
        download_files(target_folder, bucket_name, mock_s3_resource)

        # Assert
        mock_s3_resource.Bucket.assert_called_once_with(bucket_name)
        mock_bucket.objects.all.assert_called_once()
        mock_mkdir.assert_called()
        # Only the file should be downloaded, not the directory
        mock_bucket.download_file.assert_called_once_with("file1.txt", "/path/to/target/file1.txt")
        assert mock_logger.info.call_count == 1

    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_data_stream(self, mock_logger):
        """Test the upload_data_stream function."""
        # Arrange
        data_stream = BytesIO(b"test data")
        bucket_name = "test-bucket"
        file_name = "file.txt"
        file_prefix = "prefix/"
        mock_s3_client = MagicMock()

        # Act
        upload_data_stream(data_stream, bucket_name, mock_s3_client, file_name, file_prefix)

        # Assert
        mock_s3_client.upload_fileobj.assert_called_once_with(data_stream, bucket_name, "prefix/file.txt")
        mock_logger.info.assert_called_once()

    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_upload_data_stream_no_prefix(self, mock_logger):
        """Test the upload_data_stream function without a prefix."""
        # Arrange
        data_stream = BytesIO(b"test data")
        bucket_name = "test-bucket"
        file_name = "file.txt"
        mock_s3_client = MagicMock()

        # Act
        upload_data_stream(data_stream, bucket_name, mock_s3_client, file_name)

        # Assert
        mock_s3_client.upload_fileobj.assert_called_once_with(data_stream, bucket_name, "file.txt")
        mock_logger.info.assert_called_once()

    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_delete_objects(self, mock_logger):
        """Test the delete_objects function."""
        # Arrange
        bucket_name = "test-bucket"
        file_prefix = "prefix/"
        mock_s3_resource = MagicMock()
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.filter to return a list of objects
        file1 = MagicMock()
        file1.key = "prefix/file1.txt"

        file2 = MagicMock()
        file2.key = "prefix/file2.txt"

        mock_bucket.objects.filter.return_value = [file1, file2]

        # Act
        delete_objects(bucket_name, mock_s3_resource, file_prefix)

        # Assert
        mock_s3_resource.Bucket.assert_called_once_with(bucket_name)
        mock_bucket.objects.filter.assert_called_once_with(Prefix=file_prefix)
        mock_bucket.delete_objects.assert_called_once_with(
            Delete={"Objects": [{"Key": "prefix/file1.txt"}, {"Key": "prefix/file2.txt"}]}
        )
        mock_logger.info.assert_called_once()

    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_delete_objects_no_prefix(self, mock_logger):
        """Test the delete_objects function without a prefix."""
        # Arrange
        bucket_name = "test-bucket"
        mock_s3_resource = MagicMock()
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.all to return a list of objects
        file1 = MagicMock()
        file1.key = "file1.txt"

        file2 = MagicMock()
        file2.key = "file2.txt"

        mock_bucket.objects.all.return_value = [file1, file2]

        # Act
        delete_objects(bucket_name, mock_s3_resource)

        # Assert
        mock_s3_resource.Bucket.assert_called_once_with(bucket_name)
        mock_bucket.objects.all.assert_called_once()
        mock_bucket.delete_objects.assert_called_once_with(
            Delete={"Objects": [{"Key": "file1.txt"}, {"Key": "file2.txt"}]}
        )
        mock_logger.info.assert_called_once()

    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_delete_objects_empty(self, mock_logger):
        """Test the delete_objects function with no objects to delete."""
        # Arrange
        bucket_name = "test-bucket"
        mock_s3_resource = MagicMock()
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.all to return an empty list
        mock_bucket.objects.all.return_value = []

        # Act
        delete_objects(bucket_name, mock_s3_resource)

        # Assert
        mock_s3_resource.Bucket.assert_called_once_with(bucket_name)
        mock_bucket.objects.all.assert_called_once()
        mock_bucket.delete_objects.assert_not_called()
        mock_logger.info.assert_called_once()

    @patch("cosmotech.coal.aws.s3.LOGGER")
    def test_delete_objects_skip_prefix(self, mock_logger):
        """Test the delete_objects function skips the prefix itself."""
        # Arrange
        bucket_name = "test-bucket"
        file_prefix = "prefix/"
        mock_s3_resource = MagicMock()
        mock_bucket = MagicMock()
        mock_s3_resource.Bucket.return_value = mock_bucket

        # Mock bucket.objects.filter to return a list including the prefix itself
        prefix_obj = MagicMock()
        prefix_obj.key = "prefix/"

        file1 = MagicMock()
        file1.key = "prefix/file1.txt"

        mock_bucket.objects.filter.return_value = [prefix_obj, file1]

        # Act
        delete_objects(bucket_name, mock_s3_resource, file_prefix)

        # Assert
        mock_s3_resource.Bucket.assert_called_once_with(bucket_name)
        mock_bucket.objects.filter.assert_called_once_with(Prefix=file_prefix)
        # Only file1 should be deleted, not the prefix itself
        mock_bucket.delete_objects.assert_called_once_with(Delete={"Objects": [{"Key": "prefix/file1.txt"}]})
        mock_logger.info.assert_called_once()
