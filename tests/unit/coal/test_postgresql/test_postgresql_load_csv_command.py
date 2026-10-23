# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from cosmotech.coal.postgresql.native_csv_ingestion import CsvIngestionError
from cosmotech.csm_data.commands.postgres.load_csv import load_csv
from cosmotech.csm_data.commands.postgres.postgres import postgres
from cosmotech.csm_data.main import main

CONNECTION = [
    "--postgres-host",
    "h",
    "--postgres-db",
    "d",
    "--postgres-schema",
    "s",
    "--postgres-user",
    "u",
    "--postgres-password",
    "pw",
]
ENVVARS = ("CSM_RUN_ID", "POSTGRES_HOST_URI", "POSTGRES_DB_NAME", "POSTGRES_DB_SCHEMA", "POSTGRES_USER_NAME")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for key in ENVVARS + ("POSTGRES_HOST_PORT", "POSTGRES_USER_PASSWORD", "CSM_PSQL_FORCE_PASSWORD_ENCODING"):
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def mocks():
    with (
        patch("cosmotech.coal.postgresql.native_csv_ingestion.ingest_csv_file") as file_mock,
        patch("cosmotech.coal.postgresql.native_csv_ingestion.ingest_csv_folder") as folder_mock,
    ):
        yield file_mock, folder_mock


def invoke(args):
    return CliRunner().invoke(load_csv, args)


def test_command_registered():
    assert "load-csv" in postgres.commands
    assert "postgres" in main.commands


def test_file_options_passed_through(mocks):
    file_mock, folder_mock = mocks
    result = invoke(["--csv-file", "a.csv", "--table", "T", *CONNECTION])
    assert result.exit_code == 0, result.output
    folder_mock.assert_not_called()
    kwargs = file_mock.call_args.kwargs
    assert kwargs["csv_path"] == "a.csv"
    assert kwargs["table"] == "T"
    assert kwargs["table_prefix"] == "Cosmotech_"
    assert kwargs["run_id"] is None
    assert kwargs["configuration"].postgres.host == "h"
    assert kwargs["configuration"].postgres.port == 5432


def test_folder_options_passed_through(mocks):
    file_mock, folder_mock = mocks
    result = invoke(["--csv-folder", "dir", "--table-prefix", "", *CONNECTION])
    assert result.exit_code == 0, result.output
    file_mock.assert_not_called()
    assert folder_mock.call_args.kwargs["folder"] == "dir"
    assert folder_mock.call_args.kwargs["table_prefix"] == ""


def test_add_id_passes_run_id(mocks):
    file_mock, _ = mocks
    assert invoke(["--csv-file", "a.csv", "--add-id", "--run-id", "run-1", *CONNECTION]).exit_code == 0
    assert file_mock.call_args.kwargs["run_id"] == "run-1"


def test_run_id_ignored_without_add_id(mocks):
    file_mock, _ = mocks
    assert invoke(["--csv-file", "a.csv", "--run-id", "run-1", *CONNECTION]).exit_code == 0
    assert file_mock.call_args.kwargs["run_id"] is None


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--csv-file", "a.csv", "--postgres-host", "h"],
    ],
)
def test_usage_errors(mocks, args):
    file_mock, folder_mock = mocks
    result = invoke(args)
    assert result.exit_code == 2, result.output
    file_mock.assert_not_called()
    folder_mock.assert_not_called()


def test_ingestion_error_exits_non_zero(mocks):
    file_mock, _ = mocks
    file_mock.side_effect = CsvIngestionError("load failed")
    result = invoke(["--csv-file", "a.csv", *CONNECTION])
    assert result.exit_code == 1
    assert "load failed" in result.output
