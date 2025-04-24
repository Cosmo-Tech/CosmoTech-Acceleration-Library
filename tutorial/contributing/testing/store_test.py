# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
import tempfile
from unittest.mock import patch, MagicMock

import pyarrow
import pytest

from cosmotech.coal.mongodb.store import send_pyarrow_table_to_mongodb, dump_store_to_mongodb
from cosmotech.coal.store.store import Store


@pytest.fixture
def sample_table():
    """Create a sample PyArrow table for testing."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 25, 35],
    }
    return pyarrow.Table.from_pydict(data)


@pytest.fixture
def temp_store():
    """Create a temporary store for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        store = Store(store_location=temp_dir)
        yield store, temp_dir


class TestSendPyarrowTableToMongoDB:
    @patch("pymongo.MongoClient")
    def test_send_pyarrow_table_to_mongodb(self, mock_client, sample_table):
        # Set up mocks
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client.return_value.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_db.list_collection_names.return_value = []
        mock_collection.insert_many.return_value.inserted_ids = ["id1", "id2", "id3"]

        # Call the function
        result = send_pyarrow_table_to_mongodb(
            sample_table,
            "test_collection",
            "mongodb://localhost:27017",
            "test_db",
            True,
        )

        # Verify the result
        assert result == 3
        mock_client.assert_called_once_with("mongodb://localhost:27017")
        mock_client.return_value.__getitem__.assert_called_once_with("test_db")
        mock_db.list_collection_names.assert_called_once()
        mock_collection.insert_many.assert_called_once()

    @patch("pymongo.MongoClient")
    def test_send_pyarrow_table_to_mongodb_replace(self, mock_client, sample_table):
        # Set up mocks
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client.return_value.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_db.list_collection_names.return_value = ["test_collection"]
        mock_collection.insert_many.return_value.inserted_ids = ["id1", "id2", "id3"]

        # Call the function
        result = send_pyarrow_table_to_mongodb(
            sample_table,
            "test_collection",
            "mongodb://localhost:27017",
            "test_db",
            True,
        )

        # Verify the result
        assert result == 3
        mock_client.assert_called_once_with("mongodb://localhost:27017")
        mock_client.return_value.__getitem__.assert_called_once_with("test_db")
        mock_db.list_collection_names.assert_called_once()
        mock_collection.drop.assert_called_once()
        mock_collection.insert_many.assert_called_once()

    @patch("pymongo.MongoClient")
    def test_send_pyarrow_table_to_mongodb_append(self, mock_client, sample_table):
        # Set up mocks
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client.return_value.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_db.list_collection_names.return_value = ["test_collection"]
        mock_collection.insert_many.return_value.inserted_ids = ["id1", "id2", "id3"]

        # Call the function
        result = send_pyarrow_table_to_mongodb(
            sample_table,
            "test_collection",
            "mongodb://localhost:27017",
            "test_db",
            False,
        )

        # Verify the result
        assert result == 3
        mock_client.assert_called_once_with("mongodb://localhost:27017")
        mock_client.return_value.__getitem__.assert_called_once_with("test_db")
        mock_db.list_collection_names.assert_called_once()
        mock_collection.drop.assert_not_called()
        mock_collection.insert_many.assert_called_once()

    @patch("pymongo.MongoClient")
    def test_send_pyarrow_table_to_mongodb_empty(self, mock_client):
        # Set up mocks
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_client.return_value.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_db.list_collection_names.return_value = []

        # Create an empty table
        empty_table = pyarrow.Table.from_pydict({})

        # Call the function
        result = send_pyarrow_table_to_mongodb(
            empty_table,
            "test_collection",
            "mongodb://localhost:27017",
            "test_db",
            True,
        )

        # Verify the result
        assert result == 0
        mock_client.assert_called_once_with("mongodb://localhost:27017")
        mock_client.return_value.__getitem__.assert_called_once_with("test_db")
        mock_db.list_collection_names.assert_called_once()
        mock_collection.insert_many.assert_not_called()


class TestDumpStoreToMongoDB:
    @patch("cosmotech.coal.mongodb.store.send_pyarrow_table_to_mongodb")
    def test_dump_store_to_mongodb(self, mock_send, temp_store):
        store, temp_dir = temp_store

        # Add a table to the store
        sample_data = pyarrow.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [30, 25, 35],
            }
        )
        store.add_table("test_table", sample_data)

        # Set up mock
        mock_send.return_value = 3

        # Call the function
        dump_store_to_mongodb(
            temp_dir,
            "mongodb://localhost:27017",
            "test_db",
            "Cosmotech_",
            True,
        )

        # Verify the mock was called correctly
        mock_send.assert_called_once()
        args, kwargs = mock_send.call_args
        assert kwargs["collection_name"] == "Cosmotech_test_table"
        assert kwargs["mongodb_uri"] == "mongodb://localhost:27017"
        assert kwargs["mongodb_db"] == "test_db"
        assert kwargs["replace"] is True

    @patch("cosmotech.coal.mongodb.store.send_pyarrow_table_to_mongodb")
    def test_dump_store_to_mongodb_empty(self, mock_send, temp_store):
        _, temp_dir = temp_store

        # Call the function with an empty store
        dump_store_to_mongodb(
            temp_dir,
            "mongodb://localhost:27017",
            "test_db",
            "Cosmotech_",
            True,
        )

        # Verify the mock was not called
        mock_send.assert_not_called()

    @patch("cosmotech.coal.mongodb.store.send_pyarrow_table_to_mongodb")
    def test_dump_store_to_mongodb_multiple_tables(self, mock_send, temp_store):
        store, temp_dir = temp_store

        # Add multiple tables to the store
        table1 = pyarrow.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        table2 = pyarrow.Table.from_pydict(
            {
                "id": [4, 5],
                "name": ["Dave", "Eve"],
            }
        )
        store.add_table("table1", table1)
        store.add_table("table2", table2)

        # Set up mock
        mock_send.side_effect = [3, 2]

        # Call the function
        dump_store_to_mongodb(
            temp_dir,
            "mongodb://localhost:27017",
            "test_db",
            "Cosmotech_",
            True,
        )

        # Verify the mock was called correctly for each table
        assert mock_send.call_count == 2
        call_args_list = mock_send.call_args_list

        # Check first call
        args, kwargs = call_args_list[0]
        assert kwargs["collection_name"] in ["Cosmotech_table1", "Cosmotech_table2"]

        # Check second call
        args, kwargs = call_args_list[1]
        assert kwargs["collection_name"] in ["Cosmotech_table1", "Cosmotech_table2"]

        # Ensure both tables were processed
        collection_names = [
            call_args_list[0][1]["collection_name"],
            call_args_list[1][1]["collection_name"],
        ]
        assert "Cosmotech_table1" in collection_names
        assert "Cosmotech_table2" in collection_names
