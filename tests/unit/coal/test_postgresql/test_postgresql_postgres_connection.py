# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import builtins
import subprocess
import sys
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from cosmotech.coal.postgresql.postgres_connection import (
    PostgresConnectionError,
    PostgresConnectionOptionError,
    connect,
    import_psycopg,
    postgres_configuration,
    resolve_postgres_configuration,
)
from cosmotech.coal.utils.configuration import Configuration


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


def clear_env(monkeypatch):
    for key in (
        "CONFIG_FILE_PATH",
        "POSTGRES_HOST_URI",
        "POSTGRES_HOST_PORT",
        "POSTGRES_DB_NAME",
        "POSTGRES_DB_SCHEMA",
        "POSTGRES_USER_NAME",
        "POSTGRES_USER_PASSWORD",
        "CSM_PSQL_FORCE_PASSWORD_ENCODING",
    ):
        monkeypatch.delenv(key, raising=False)


class TestImportPsycopg:
    def test_import_psycopg_failure_gives_dedicated_error_with_cause(self):
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "psycopg":
                raise ImportError("no pq wrapper available")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            with pytest.raises(PostgresConnectionError) as excinfo:
                import_psycopg()
        assert isinstance(excinfo.value.__cause__, ImportError)
        assert "psycopg[binary]" in str(excinfo.value)

    def test_import_psycopg_package_import_does_not_need_psycopg(self):
        code = (
            "import sys; sys.modules['psycopg'] = None; "
            "import cosmotech.coal.postgresql, cosmotech.csm_data.main; "
            "print('ok')"
        )
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        assert "ok" in result.stdout


class TestConnect:
    def test_connect_sets_search_path(self, configuration):
        conn = MagicMock()
        with patch("psycopg.connect", return_value=conn) as connect_mock:
            assert connect(configuration) is conn
        assert connect_mock.call_args.args[0] == "postgresql://user:p%40ss%20word@localhost:5432/testdb"
        assert connect_mock.call_args.kwargs == {"autocommit": False}
        assert conn.execute.call_args.args[0].as_string() == 'SET search_path TO "dbschema"'

    def test_connect_failure_has_no_password(self, configuration):
        error = psycopg.OperationalError("failed with p@ss word and p%40ss%20word")
        with patch("psycopg.connect", side_effect=error):
            with pytest.raises(PostgresConnectionError) as excinfo:
                connect(configuration)
        assert "p@ss word" not in str(excinfo.value)
        assert "p%40ss%20word" not in str(excinfo.value)
        assert excinfo.value.__cause__ is None


class TestResolvePostgresConfiguration:
    ALL = dict(host="h", db="d", schema="s", user="u", password="pw")

    def test_resolve_postgres_configuration_all_options(self, monkeypatch):
        clear_env(monkeypatch)
        conf = resolve_postgres_configuration(port=6543, encode_password=False, **self.ALL)
        assert conf.postgres.host == "h"
        assert conf.postgres.port == 6543
        assert conf.postgres.db_name == "d"
        assert conf.postgres.db_schema == "s"
        assert conf.postgres.user_name == "u"
        assert conf.postgres.user_password == "pw"
        assert conf.postgres.password_encoding is False
        assert resolve_postgres_configuration(**self.ALL).postgres.port == 5432

    def test_resolve_postgres_configuration_all_options_without_password(self, monkeypatch):
        clear_env(monkeypatch)
        with pytest.raises(PostgresConnectionOptionError, match="password"):
            resolve_postgres_configuration(host="h", db="d", schema="s", user="u")

    def test_resolve_postgres_configuration_partial_options_name_missing_ones(self, monkeypatch):
        clear_env(monkeypatch)
        monkeypatch.setenv("POSTGRES_DB_NAME", "from_env")
        with pytest.raises(PostgresConnectionOptionError) as excinfo:
            resolve_postgres_configuration(host="h", user="u", password="pw")
        assert "--postgres-db" in str(excinfo.value)
        assert "--postgres-schema" in str(excinfo.value)
        assert "--postgres-host" not in str(excinfo.value)

    def test_resolve_postgres_configuration_none_uses_environment(self, monkeypatch):
        clear_env(monkeypatch)
        for key, value in {
            "POSTGRES_HOST_URI": "envhost",
            "POSTGRES_DB_NAME": "envdb",
            "POSTGRES_DB_SCHEMA": "envschema",
            "POSTGRES_USER_NAME": "envuser",
            "POSTGRES_USER_PASSWORD": "envpw",
        }.items():
            monkeypatch.setenv(key, value)
        conf = resolve_postgres_configuration()
        assert conf.postgres.host == "envhost"
        assert conf.postgres.db_schema == "envschema"
        assert conf.postgres.user_password == "envpw"
        assert conf.postgres.port == 5432

    def test_resolve_postgres_configuration_none_uses_toml_with_password_from_env(self, monkeypatch, tmp_path):
        clear_env(monkeypatch)
        toml = tmp_path / "coal-config.toml"
        toml.write_text(
            "[[outputs]]\n"
            'type = "postgres"\n'
            "[outputs.conf.postgres]\n"
            'host = "tomlhost"\n'
            'port = "5433"\n'
            'db_name = "tomldb"\n'
            'db_schema = "tomlschema"\n'
            'user_name = "tomluser"\n'
            'user_password = "env.POSTGRES_USER_PASSWORD"\n'
        )
        monkeypatch.setenv("CONFIG_FILE_PATH", str(toml))
        monkeypatch.setenv("POSTGRES_USER_PASSWORD", "cluster-pw")
        conf = resolve_postgres_configuration()
        assert conf.postgres.host == "tomlhost"
        assert conf.postgres.port == "5433"
        assert conf.postgres.user_password == "cluster-pw"

    def test_resolve_postgres_configuration_nothing_usable(self, monkeypatch):
        clear_env(monkeypatch)
        with pytest.raises(PostgresConnectionOptionError) as excinfo:
            resolve_postgres_configuration()
        assert "host" in str(excinfo.value)
        assert "user_password" in str(excinfo.value)


def test_postgres_configuration_uses_postgres_output():
    base = Configuration({"outputs": [{"type": "postgres", "conf": {"postgres": {"host": "tomlhost"}}}]})
    assert postgres_configuration(base).postgres.host == "tomlhost"
