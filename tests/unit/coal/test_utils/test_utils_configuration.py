# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use reproduction translation broadcasting transmission distribution
# etc. to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os

import pytest

from cosmotech.coal.utils import configuration
from cosmotech.coal.utils.configuration import Dotdict


class TestUtilsConfiguration:

    def test_no_config_file(self):
        c = configuration.Configuration()
        assert isinstance(c, Dotdict)

    def test_no_config_file_with_env_var(self):
        os.environ["LOG_LEVEL"] = "test_value"
        c = configuration.Configuration()

        # assert there no secrets section (must have been replaced
        with pytest.raises(KeyError):
            c.secrets
        assert c.log_level == "test_value"

    def test_config_file_with_secrets(self):

        os.environ["CONFIG_FILE_PATH"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "conf.ini"))
        os.environ["faismoinlmalin"] = "la"

        c = configuration.Configuration()

        # assert there no secrets section (must have been replaced
        with pytest.raises(KeyError):
            c.secrets
        assert c.foo.alors == "la"


class TestUtilsDotdict:

    def test_nested_access(self):
        dict_a = {"lvl1": {"lvl2": {"lvl3": "here"}}}
        dotdict_a = Dotdict(dict_a)

        assert dotdict_a.lvl1.lvl2.lvl3 == "here"

    def test_nested_merge(self):
        boulangerie1 = {"pain": {"baguette": 2, "noix": 1}, "croissant": 5}
        boulangerie2 = {"pain": {"baguette": 4, "sesame": 4}, "chocolatine": 5}

        db1 = Dotdict(boulangerie1)
        db2 = Dotdict(boulangerie2)

        db1.merge(db2)

        print(db1)
        expected = {"pain": {"baguette": 4, "sesame": 4, "noix": 1}, "croissant": 5, "chocolatine": 5}
        assert db1 == expected
