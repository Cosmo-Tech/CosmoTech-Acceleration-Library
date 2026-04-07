# Copyright (C) - 2022 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pyarrow as pa
import pytest

from cosmotech.coal.store.store import Store


@pytest.fixture(scope="function")
def store():
    store = Store(reset=True)
    yield store
    store.reset()


class TestIntegrationStore:
    """Tests for the store class."""

    def test_get_table(self, store):
        """Test get table with table name starting with numbers"""

        # Arrange
        table_name = "normal_name"
        table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        store.add_table(table_name, table)

        # Act
        result = store.get_table(table_name)

        # Assert
        assert result

    def test_get_table_with_number_name(self, store):
        """Test get table with table name starting with numbers"""

        # Arrange
        table_name = "10mb"
        table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        store.add_table(table_name, table)

        # Act
        result = store.get_table(table_name)

        # Assert
        assert result

    def test_add_get_table_with_upper_and_lower_case(self, store):
        """Test add table and get table behavior with uppper and lower cases"""

        # Arrange

        table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        store.add_table("10mb", table)
        table = pa.Table.from_arrays([pa.array([4, 5, 6]), pa.array(["A", "B", "C"])], names=["id", "name"])
        store.add_table("10MB", table)

        # Act
        UPPER_result = store.get_table("10MB")
        upper_result = store.get_table("10mb")

        assert upper_result
        assert UPPER_result
        assert upper_result == UPPER_result

    def test_execute_query_without_parameters(self, store):
        """Test execute_query with a plain SQL query and no parameters"""

        # Arrange
        table_name = "items"
        table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        store.add_table(table_name, table)

        # Act
        result = store.execute_query(f'SELECT * FROM "{table_name}" ORDER BY id')

        # Assert
        assert result is not None
        assert result.num_rows == 3
        assert result.column("id").to_pylist() == [1, 2, 3]
        assert result.column("name").to_pylist() == ["a", "b", "c"]

    def test_execute_query_with_parameters(self, store):
        """Test execute_query with a parameterized SQL query"""

        # Arrange
        table_name = "items"
        table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        store.add_table(table_name, table)

        # Act
        result = store.execute_query(f'SELECT * FROM "{table_name}" WHERE id = ?', parameters=[2])

        # Assert
        assert result is not None
        assert result.num_rows == 1
        assert result.column("id").to_pylist() == [2]
        assert result.column("name").to_pylist() == ["b"]
