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


class TestStore:
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
