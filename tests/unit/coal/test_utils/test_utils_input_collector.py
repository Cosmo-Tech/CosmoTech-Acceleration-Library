# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
from unittest.mock import MagicMock, patch

import pytest

from cosmotech.coal.utils.input_collector import (
    DatasetCollector,
    InputCollector,
    ParameterCollector,
)


@pytest.fixture
def mock_ec(tmp_path):
    """Fixture that patches EC with a MagicMock whose dataset and parameters paths
    both point to tmp_path. Override the attributes in individual tests as needed."""
    ec = MagicMock()
    ec.cosmotech.dataset_absolute_path = str(tmp_path)
    ec.cosmotech.parameters_absolute_path = str(tmp_path)
    with patch("cosmotech.coal.utils.input_collector.EC", ec):
        yield ec


class TestDatasetCollector:
    def test_fetch_existing_file(self, tmp_path, mock_ec):
        dataset_dir = tmp_path / "ds1"
        dataset_dir.mkdir()
        file = dataset_dir / "mydata.csv"
        file.write_text("a,b\n1,2")

        mock_ec.cosmotech.dataset_absolute_path = str(tmp_path)

        collector = DatasetCollector()
        result = collector.fetch("mydata.csv")

        assert result == file

    def test_fetch_triggers_lazy_collection(self, tmp_path, mock_ec):
        dataset_dir = tmp_path / "ds1"
        dataset_dir.mkdir()
        (dataset_dir / "file.csv").write_text("")

        mock_ec.cosmotech.dataset_absolute_path = str(tmp_path)

        collector = DatasetCollector()
        assert collector.paths == {}
        collector.fetch("file.csv")
        assert "file.csv" in collector.paths

    def test_fetch_missing_file_raises(self, tmp_path, mock_ec):
        dataset_dir = tmp_path / "ds1"
        dataset_dir.mkdir()

        mock_ec.cosmotech.dataset_absolute_path = str(tmp_path)

        collector = DatasetCollector()
        with pytest.raises(FileNotFoundError):
            collector.fetch("nonexistent.csv")

    def test_collect_indexes_nested_files(self, tmp_path, mock_ec):
        sub = tmp_path / "ds1" / "sub"
        sub.mkdir(parents=True)
        f = sub / "nested.csv"
        f.write_text("")

        mock_ec.cosmotech.dataset_absolute_path = str(tmp_path)

        collector = DatasetCollector()
        collector.collect()

        assert "nested.csv" in collector.paths
        assert collector.paths["nested.csv"] == f


class TestParameterCollector:
    def test_init_starts_with_empty_dicts(self, tmp_path, mock_ec):
        """parameters.json is no longer read at init — loading is now lazy."""
        params = [{"parameterId": "alpha", "value": "42"}]
        (tmp_path / "parameters.json").write_text(json.dumps(params))

        collector = ParameterCollector()

        assert collector.parameters == {}
        assert collector.paths == {}

    def test_read_parameters_json_populates_parameters(self, tmp_path, mock_ec):
        params = [{"parameterId": "alpha", "value": "42"}, {"parameterId": "beta", "value": "hello"}]
        (tmp_path / "parameters.json").write_text(json.dumps(params))

        collector = ParameterCollector()
        collector.read_parameters_json()

        assert collector.parameters["alpha"] == "42"
        assert collector.parameters["beta"] == "hello"

    def test_read_parameters_json_no_file_keeps_empty(self, tmp_path, mock_ec):
        collector = ParameterCollector()
        collector.read_parameters_json()

        assert collector.parameters == {}

    def test_fetch_parameter_triggers_lazy_load(self, tmp_path, mock_ec):
        params = [{"parameterId": "myparam", "value": "myvalue"}]
        (tmp_path / "parameters.json").write_text(json.dumps(params))

        collector = ParameterCollector()
        assert collector.parameters == {}

        result = collector.fetch_parameter("myparam")

        assert result == "myvalue"
        assert "myparam" in collector.parameters

    def test_fetch_parameter_does_not_reload_if_already_loaded(self, tmp_path, mock_ec):
        params = [{"parameterId": "key", "value": "first"}]
        (tmp_path / "parameters.json").write_text(json.dumps(params))

        collector = ParameterCollector()
        collector.read_parameters_json()
        # Mutate to confirm no second load overwrites it
        collector.parameters["key"] = "modified"

        result = collector.fetch_parameter("key")

        assert result == "modified"

    def test_fetch_parameter_raises_key_error_for_unknown(self, tmp_path, mock_ec):
        (tmp_path / "parameters.json").write_text(json.dumps([]))

        collector = ParameterCollector()
        with pytest.raises(KeyError):
            collector.fetch_parameter("nonexistent")

    def test_fetch_file_path_returns_file(self, tmp_path, mock_ec):
        param_dir = tmp_path / "myparam"
        param_dir.mkdir()
        f = param_dir / "data.csv"
        f.write_text("")

        collector = ParameterCollector()
        result = collector.fetch_file_path("myparam")

        assert result == f

    def test_fetch_file_path_missing_raises(self, tmp_path, mock_ec):
        collector = ParameterCollector()
        with pytest.raises(FileNotFoundError):
            collector.fetch_file_path("nonexistent")

    def test_fetch_returns_value_if_already_in_parameters(self, tmp_path, mock_ec):
        """fetch() checks self.parameters directly — no lazy load triggered."""
        collector = ParameterCollector()
        collector.parameters["preloaded"] = "value"

        result = collector.fetch("preloaded")

        assert result == "value"

    def test_fetch_falls_back_to_file_without_lazy_load(self, tmp_path, mock_ec):
        """fetch() does NOT trigger read_parameters_json — falls straight to fetch_file_path."""
        param_dir = tmp_path / "myparam"
        param_dir.mkdir()
        f = param_dir / "data.csv"
        f.write_text("")

        # parameters.json exists but fetch() won't load it
        (tmp_path / "parameters.json").write_text(json.dumps([{"parameterId": "myparam", "value": "json_val"}]))

        collector = ParameterCollector()
        result = collector.fetch("myparam")

        assert result == "json_val"


class TestInputCollector:
    def test_fetch_parameter_returns_preloaded_value(self, tmp_path, mock_ec):
        """InputCollector.fetch_parameter calls parameter_collector.fetch(),
        which checks self.parameters directly without lazy-loading JSON."""
        collector = InputCollector()
        collector.parameter_collector.parameters["key"] = "val"

        assert collector.fetch_parameter("key") == "val"

    def test_fetch_parameter_falls_back_to_file(self, tmp_path, mock_ec):
        param_dir = tmp_path / "myparam"
        param_dir.mkdir()
        f = param_dir / "data.csv"
        f.write_text("")

        collector = InputCollector()
        result = collector.fetch_parameter("myparam")

        assert result == f

    def test_fetch_dataset_delegates_to_dataset_collector(self, tmp_path, mock_ec):
        ds_dir = tmp_path / "ds" / "ds1"
        ds_dir.mkdir(parents=True)
        f = ds_dir / "myfile.csv"
        f.write_text("")

        mock_ec.cosmotech.dataset_absolute_path = str(tmp_path / "ds")

        collector = InputCollector()
        result = collector.fetch_dataset("myfile.csv")

        assert result == f

    def test_fetch_tries_parameter_first(self, tmp_path, mock_ec):
        """fetch() catches KeyError/FileNotFoundError from fetch_parameter and falls back to dataset."""
        ds_dir = tmp_path / "ds" / "ds1"
        ds_dir.mkdir(parents=True)
        f = ds_dir / "fallback.csv"
        f.write_text("")

        mock_ec.cosmotech.dataset_absolute_path = str(tmp_path / "ds")

        collector = InputCollector()
        # Pre-load a parameter so fetch_parameter returns it without touching files
        collector.parameter_collector.parameters["fallback.csv"] = "param_value"

        result = collector.fetch("fallback.csv")

        assert result == "param_value"

    def test_fetch_falls_back_to_dataset(self, tmp_path, mock_ec):
        ds_dir = tmp_path / "ds" / "ds1"
        ds_dir.mkdir(parents=True)
        f = ds_dir / "fallback.csv"
        f.write_text("")

        mock_ec.cosmotech.dataset_absolute_path = str(tmp_path / "ds")

        collector = InputCollector()
        result = collector.fetch("fallback.csv")

        assert result == f
