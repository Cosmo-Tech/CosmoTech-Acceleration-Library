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
from cosmotech.csm_data.commands.custom.custom import custom
from cosmotech.csm_data.commands.custom.load_csv_folder import load_csv_folder

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
ENVVARS = (
    "CSM_OUTPUT_ABSOLUTE_PATH",
    "CSM_RUN_ID",
    "POSTGRES_HOST_URI",
    "POSTGRES_HOST_PORT",
    "POSTGRES_DB_NAME",
    "POSTGRES_DB_SCHEMA",
    "POSTGRES_USER_NAME",
    "POSTGRES_USER_PASSWORD",
    "CSM_PSQL_FORCE_PASSWORD_ENCODING",
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for key in ENVVARS:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def ingest():
    with patch("cosmotech.coal.postgresql.native_csv_ingestion.ingest_csv_folder") as mock:
        yield mock


def invoke(args):
    return CliRunner().invoke(load_csv_folder, args)


def test_command_registered():
    assert "load-csv-folder" in custom.commands


def test_options_passed_through(ingest):
    result = invoke(["--csv-folder", "dir", "--table-prefix", "", *CONNECTION])
    assert result.exit_code == 0, result.output
    kwargs = ingest.call_args.kwargs
    assert kwargs["folder"] == "dir"
    assert kwargs["table_prefix"] == ""
    assert kwargs["run_id"] is None
    assert kwargs["configuration"].postgres.host == "h"


def test_csv_folder_default_from_environment(ingest, monkeypatch):
    monkeypatch.setenv("CSM_OUTPUT_ABSOLUTE_PATH", "env_dir")
    assert invoke(CONNECTION).exit_code == 0
    assert ingest.call_args.kwargs["folder"] == "env_dir"


def test_default_table_prefix(ingest):
    assert invoke(["--csv-folder", "dir", *CONNECTION]).exit_code == 0
    assert ingest.call_args.kwargs["table_prefix"] == "Cosmotech_"


def test_add_id_passes_run_id(ingest):
    assert invoke(["--csv-folder", "dir", "--add-id", "--run-id", "run-1", *CONNECTION]).exit_code == 0
    assert ingest.call_args.kwargs["run_id"] == "run-1"


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--csv-folder", "dir", "--table", "T", *CONNECTION],
        ["--csv-folder", "dir", "--add-id", *CONNECTION],
        ["--csv-folder", "dir", "--postgres-host", "h"],
    ],
)
def test_usage_errors(ingest, args):
    result = invoke(args)
    assert result.exit_code == 2, result.output
    ingest.assert_not_called()


def test_ingestion_error_exits_non_zero(ingest):
    ingest.side_effect = CsvIngestionError("load failed")
    result = invoke(["--csv-folder", "dir", *CONNECTION])
    assert result.exit_code == 1
    assert "load failed" in result.output
