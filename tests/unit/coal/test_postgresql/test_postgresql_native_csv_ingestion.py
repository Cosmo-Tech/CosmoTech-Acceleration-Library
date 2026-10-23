# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from cosmotech.coal.postgresql import native_csv_ingestion as pg_csv
from cosmotech.coal.postgresql.native_csv_ingestion import (
    CsvIngestionError,
    CsvIngestionOptionError,
    build_copy_statement,
    check_csv_file,
    ingest_csv_file,
    ingest_csv_folder,
    list_csv_files,
    preflight,
    read_header,
    resolve_table_name,
    validate_options,
)
from cosmotech.coal.utils.configuration import Configuration

RUN_ID_DEFAULT = "current_setting('csm.run_id'::text)"


@pytest.fixture
def configuration():
    return Configuration(
        {
            "postgres": {
                "host": "localhost",
                "port": "5432",
                "db_name": "testdb",
                "db_schema": "dbschema",
                "user_name": "user",
                "user_password": "p@ss word",
                "password_encoding": True,
            }
        }
    )


@pytest.fixture
def csv_file(tmp_path):
    path = tmp_path / "Data.csv"
    path.write_text("a,B\n1,2\n3,4\n")
    return path


def make_catalog_conn(relkind="r", columns=None, rowcount=2):
    """
    Mocked connection answering the catalog queries of the preflight and exposing a cursor for the load.

    The queries are recognised by a fragment of their SQL, so rewording a query in the preflight breaks these tests.
    """
    columns = columns if columns is not None else [("a", None), ("B", None)]
    conn = MagicMock()
    conn.__enter__.return_value = conn

    def execute(query, params=None):
        result = MagicMock()
        if "relkind" in query:
            result.fetchone.return_value = None if relkind is None else (relkind,)
        elif "pg_attribute" in query:
            result.fetchall.return_value = columns
        return result

    conn.execute.side_effect = execute
    conn.cursor.return_value.__enter__.return_value.rowcount = rowcount
    return conn


def cursor_of(conn):
    return conn.cursor.return_value.__enter__.return_value


def written_chunks(conn):
    copy = cursor_of(conn).copy.return_value.__enter__.return_value
    return [c.args[0] for c in copy.write.call_args_list]


class TestValidateOptions:
    def test_validate_options_valid(self):
        validate_options(csv_file="a.csv", table="t")
        validate_options(csv_folder="dir", table_prefix="x_")
        validate_options(csv_file="a.csv", add_id=True, run_id="run-1")

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"csv_file": "a.csv", "csv_folder": "dir"},
            {"csv_folder": "dir", "table": "t"},
            {"csv_file": "a.csv", "table": "t", "table_prefix": ""},
            {"csv_file": "a.csv", "add_id": True},
        ],
    )
    def test_validate_options_invalid(self, kwargs):
        with pytest.raises(CsvIngestionOptionError):
            validate_options(**kwargs)


class TestResolveTableName:
    @pytest.mark.parametrize(
        "args, expected",
        [
            ((Path("x/Data.csv"), "MyTable", "pre_"), "MyTable"),
            ((Path("x/Data.csv"), None, "Cosmotech_"), "cosmotech_Data"),
            ((Path("Data.csv"), None, ""), "Data"),
        ],
    )
    def test_resolve_table_name(self, args, expected):
        assert resolve_table_name(*args) == expected


class TestReadHeader:
    def test_read_header_bom_stripped(self, tmp_path):
        path = tmp_path / "bom.csv"
        path.write_bytes(b"\xef\xbb\xbfa,b\n1,2\n")
        assert read_header(path) == ["a", "b"]

    def test_read_header_empty_file(self, tmp_path):
        path = tmp_path / "empty.csv"
        path.write_text("")
        assert read_header(path) == []


class TestFileChecks:
    @pytest.mark.parametrize("content", [None, ""])
    def test_check_csv_file_missing_or_empty(self, tmp_path, content):
        path = tmp_path / "x.csv"
        if content is not None:
            path.write_text(content)
        with pytest.raises(CsvIngestionError):
            check_csv_file(path)

    def test_list_csv_files_sorted_and_filtered(self, tmp_path):
        for name in ("b.csv", "a.csv", "notes.txt"):
            (tmp_path / name).write_text("x\n")
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "c.csv").write_text("x\n")
        assert [p.name for p in list_csv_files(tmp_path)] == ["a.csv", "b.csv"]

    @pytest.mark.parametrize("folder", [".", "missing"])
    def test_list_csv_files_no_csv_or_not_a_folder(self, tmp_path, folder):
        with pytest.raises(CsvIngestionError):
            list_csv_files(tmp_path / folder)


def test_build_copy_statement_quotes_identifiers_and_preserves_case():
    statement = build_copy_statement("MyTable", ["a", "B c", 'd"e'], schema="Sch")
    assert (
        statement.as_string() == 'COPY "Sch"."MyTable" ("a", "B c", "d""e") FROM STDIN WITH (FORMAT csv, HEADER true)'
    )


class TestPreflight:
    def test_preflight_ok_returns_columns(self, csv_file):
        conn = make_catalog_conn()
        assert preflight(conn, csv_file, "Data", False, "dbschema") == ["a", "B"]

    def test_preflight_mixed_case_table_passed_as_plain_string(self, csv_file):
        conn = make_catalog_conn()
        preflight(conn, csv_file, "MixedCase", False, "dbschema")
        assert all(call.args[1] == ("dbschema", "MixedCase") for call in conn.execute.call_args_list)

    def test_preflight_table_missing_names_the_table(self, csv_file):
        conn = make_catalog_conn(relkind=None)
        with pytest.raises(CsvIngestionError, match="MyTable"):
            preflight(conn, csv_file, "MyTable", False, "dbschema")

    def test_preflight_not_a_table(self, csv_file):
        conn = make_catalog_conn(relkind="v")
        with pytest.raises(CsvIngestionError, match="not a table"):
            preflight(conn, csv_file, "Data", False, "dbschema")

    def test_preflight_unknown_columns_reported_together(self, tmp_path):
        path = tmp_path / "x.csv"
        path.write_text("a,x,y\n1,2,3\n")
        conn = make_catalog_conn()
        with pytest.raises(CsvIngestionError) as excinfo:
            preflight(conn, path, "Data", False, "dbschema")
        assert "x, y" in str(excinfo.value)

    @pytest.mark.parametrize("content", ["\n1,2\n", ",a\n1,2\n", "a,a\n1,2\n"])
    def test_preflight_header_sanity(self, tmp_path, content):
        path = tmp_path / "x.csv"
        path.write_text(content)
        conn = make_catalog_conn()
        with pytest.raises(CsvIngestionError):
            preflight(conn, path, "Data", False, "dbschema")
        conn.execute.assert_not_called()

    def test_preflight_add_id_ok(self, csv_file):
        conn = make_catalog_conn(columns=[("a", None), ("B", None), ("csm_run_id", RUN_ID_DEFAULT)])
        assert preflight(conn, csv_file, "Data", True, "dbschema") == ["a", "B"]

    def test_preflight_add_id_with_run_id_in_header(self, tmp_path):
        path = tmp_path / "x.csv"
        path.write_text("a,csm_run_id\n1,2\n")
        conn = make_catalog_conn(columns=[("a", None), ("csm_run_id", RUN_ID_DEFAULT)])
        with pytest.raises(CsvIngestionError, match="csm_run_id"):
            preflight(conn, path, "Data", True, "dbschema")

    def test_preflight_add_id_column_missing(self, csv_file):
        with pytest.raises(CsvIngestionError, match="no csm_run_id column"):
            preflight(make_catalog_conn(), csv_file, "Data", True, "dbschema")

    @pytest.mark.parametrize("default", [None, "'fixed'::text"])
    def test_preflight_add_id_without_run_id_default(self, csv_file, default):
        conn = make_catalog_conn(columns=[("a", None), ("B", None), ("csm_run_id", default)])
        with pytest.raises(CsvIngestionError, match="current_setting"):
            preflight(conn, csv_file, "Data", True, "dbschema")


class TestIngestCsvFile:
    def run(self, csv_file, conn, configuration, **kwargs):
        with patch("cosmotech.coal.postgresql.native_csv_ingestion.connect", return_value=conn):
            return ingest_csv_file(csv_file, configuration, **kwargs)

    def test_ingest_csv_file_commits_and_returns_rowcount(self, csv_file, configuration):
        conn = make_catalog_conn(rowcount=2)
        assert self.run(csv_file, conn, configuration) == 2
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
        cursor_of(conn).execute.assert_not_called()

    def test_ingest_csv_file_copy_statement_is_qualified_and_quoted(self, csv_file, configuration):
        conn = make_catalog_conn()
        self.run(csv_file, conn, configuration, table="Other")
        statement = cursor_of(conn).copy.call_args.args[0].as_string()
        assert statement == 'COPY "dbschema"."Other" ("a", "B") FROM STDIN WITH (FORMAT csv, HEADER true)'

    def test_ingest_csv_file_run_id_sets_transaction_local_setting(self, csv_file, configuration):
        conn = make_catalog_conn(columns=[("a", None), ("B", None), ("csm_run_id", RUN_ID_DEFAULT)])
        self.run(csv_file, conn, configuration, run_id="run-1")
        cursor_of(conn).execute.assert_called_once_with("SELECT set_config(%s, %s, true)", ("csm.run_id", "run-1"))

    def test_ingest_csv_file_failure_during_copy(self, csv_file, configuration):
        conn = make_catalog_conn()
        cursor_of(conn).copy.return_value.__enter__.return_value.write.side_effect = psycopg.errors.BadCopyFileFormat(
            "bad line"
        )
        with pytest.raises(CsvIngestionError, match="Data.csv"):
            self.run(csv_file, conn, configuration)
        conn.commit.assert_not_called()
        conn.__exit__.assert_called_once()

    def test_ingest_csv_file_preflight_failure_sends_nothing(self, csv_file, configuration):
        conn = make_catalog_conn(relkind=None)
        with pytest.raises(CsvIngestionError):
            self.run(csv_file, conn, configuration)
        conn.cursor.assert_not_called()
        conn.commit.assert_not_called()

    def test_ingest_csv_file_chunking(self, tmp_path, configuration):
        path = tmp_path / "big.csv"
        content = "a,B\n" + "".join(f"{i},{i}\n" for i in range(1000))
        path.write_text(content)
        conn = make_catalog_conn()
        with patch.object(pg_csv, "CHUNK_SIZE", 100):
            self.run(path, conn, configuration, table="t")
        chunks = written_chunks(conn)
        assert len(chunks) > 1
        assert b"".join(chunks) == content.encode()

    def test_ingest_csv_file_header_only_warns(self, tmp_path, configuration):
        path = tmp_path / "h.csv"
        path.write_text("a,B\n")
        conn = make_catalog_conn(rowcount=0)
        with patch.object(pg_csv.LOGGER, "warning") as warning:
            assert self.run(path, conn, configuration, table="t") == 0
        warning.assert_called_once()
        conn.commit.assert_called_once()


class TestIngestCsvFolder:
    @pytest.fixture
    def folder(self, tmp_path):
        for name in ("b.csv", "A.csv"):
            (tmp_path / name).write_text("a,B\n1,2\n")
        (tmp_path / "notes.txt").write_text("ignored")
        return tmp_path

    def run(self, folder, connections, configuration, **kwargs):
        with patch("cosmotech.coal.postgresql.native_csv_ingestion.connect", side_effect=connections) as connect_mock:
            result = ingest_csv_folder(folder, configuration, **kwargs)
        return result, connect_mock

    def test_ingest_csv_folder_loads_each_file_in_its_own_transaction(self, folder, configuration):
        preflight_conn = make_catalog_conn()
        load_conns = [make_catalog_conn(rowcount=1), make_catalog_conn(rowcount=5)]
        result, connect_mock = self.run(folder, [preflight_conn, *load_conns], configuration)

        assert result == {"cosmotech_A": 1, "cosmotech_b": 5}
        assert connect_mock.call_count == 3
        tables = [call.args[1][1] for call in preflight_conn.execute.call_args_list if "relkind" in call.args[0]]
        assert tables == ["cosmotech_A", "cosmotech_b"]
        for conn in load_conns:
            conn.commit.assert_called_once()
        assert preflight_conn.commit.call_count == 0

    def test_ingest_csv_folder_preflight_of_all_files_before_first_load(self, folder, configuration):
        preflight_conn = make_catalog_conn(relkind=None)
        with patch(
            "cosmotech.coal.postgresql.native_csv_ingestion.connect", side_effect=[preflight_conn]
        ) as connect_mock:
            with pytest.raises(CsvIngestionError, match="cosmotech_A"):
                ingest_csv_folder(folder, configuration)
        assert connect_mock.call_count == 1

    def test_ingest_csv_folder_empty_file_detected_before_connecting(self, folder, configuration):
        (folder / "c.csv").write_text("")
        with patch("cosmotech.coal.postgresql.native_csv_ingestion.connect") as connect_mock:
            with pytest.raises(CsvIngestionError):
                ingest_csv_folder(folder, configuration)
        connect_mock.assert_not_called()

    def test_ingest_csv_folder_failure_logs_loaded_failed_and_not_started(self, tmp_path, configuration):
        for name in ("a.csv", "b.csv", "c.csv"):
            (tmp_path / name).write_text("a,B\n1,2\n")
        failing = make_catalog_conn()
        cursor_of(failing).copy.return_value.__enter__.return_value.write.side_effect = psycopg.errors.DataException(
            "bad value"
        )
        connections = [make_catalog_conn(), make_catalog_conn(), failing]
        with patch.object(pg_csv.LOGGER, "error") as error:
            with pytest.raises(CsvIngestionError):
                self.run(tmp_path, connections, configuration)
        messages = [call.args[0] for call in error.call_args_list]
        assert len(messages) == 3
        assert "loaded before the failure" in messages[0] and "cosmotech_a" in messages[0]
        assert "in error" in messages[1] and "cosmotech_b" in messages[1]
        assert "not started" in messages[2] and "cosmotech_c" in messages[2]
