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

    @pytest.fixture(autouse=True)
    def reset_environ(self):
        if "CONFIG_FILE_PATH" in os.environ:
            os.environ.pop("CONFIG_FILE_PATH")
        if "LOG_LEVEL" in os.environ:
            os.environ.pop("LOG_LEVEL")
        yield

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

    def test_config_file_with_ref(self):
        os.environ["CONFIG_FILE_PATH"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "conf.ini"))
        c = configuration.Configuration()
        c.section.ref = "$section.sub.TEST"

        assert c.section.ref == c.section.sub.TEST

    def test_safe_get(self):
        os.environ["LOG_LEVEL"] = "test_value"
        c = configuration.Configuration()

        assert c.safe_get("log_level") == "test_value"

    def test_safe_get_none_dict(self):
        os.environ["LOG_LEVEL"] = "test_value"
        c = configuration.Configuration()

        assert c.safe_get("log_level.test") is None

    def test_safe_get_sub_section(self):
        os.environ["CONFIG_FILE_PATH"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "conf.ini"))
        c = configuration.Configuration()

        assert c.safe_get("DEFAULT.A.test") == 1

    def test_safe_get_default_value(self):
        c = configuration.Configuration()

        assert c.safe_get("log_level", "thats_a_no") == "thats_a_no"


class TestUtilsDotdict:

    def test_nested_access(self):
        dict_a = {"lvl1": {"lvl2": {"lvl3": "here"}}}
        dotdict_a = Dotdict(dict_a)

        assert dotdict_a.lvl1.lvl2.lvl3 == "here"

    def test_unknow_key_error(self):
        dict_a = {"lvl1": {"lvl2": {"lvl3": "here"}}}
        dotdict_a = Dotdict(dict_a)

        with pytest.raises(KeyError):
            dotdict_a.lvl1.lvl2.lvl99

    def test_nested_merge(self):
        boulangerie1 = {"pain": {"baguette": 2, "noix": 1}, "croissant": 5}
        boulangerie2 = {"pain": {"baguette": 4, "sesame": 4}, "chocolatine": 5}

        db1 = Dotdict(boulangerie1)
        db2 = Dotdict(boulangerie2)

        db1.merge(db2)

        print(db1)
        expected = {"pain": {"baguette": 4, "sesame": 4, "noix": 1}, "croissant": 5, "chocolatine": 5}
        assert db1 == expected

    def test_ref_value(self):
        dict_a = {"lvl1": {"lvl2": {"lvl3": "here"}}, "ref": {"ref_lvl3": "$lvl1.lvl2.lvl3"}}
        dotdict_a = Dotdict(dict_a)

        assert dotdict_a.ref.ref_lvl3 == "here"

    def test_ref_dict_lvl(self):
        dict_a = {"lvl1": {"lvl2": {"lvl3": "here"}}, "ref": {"ref_lvl2": "$lvl1.lvl2"}}
        dotdict_a = Dotdict(dict_a)

        assert dotdict_a.ref.ref_lvl2 == {"lvl3": "here"}

    def test_ref_update(self):
        dict_a = {"lvl1": {"lvl2": {"lvl3": "here"}}, "ref": {"ref_lvl3": "$lvl1.lvl2.lvl3"}}
        dotdict_a = Dotdict(dict_a)

        assert dotdict_a.ref.ref_lvl3 == "here"
        dotdict_a.lvl1.lvl2.lvl3 = "there"
        assert dotdict_a.ref.ref_lvl3 == "there"

    def test_ref_dict_update(self):
        dict_a = {"lvl1": {"lvl2": {"lvl3": "here"}}, "ref": {"ref_lvl2": "$lvl1.lvl2"}}
        dotdict_a = Dotdict(dict_a)

        assert dotdict_a.ref.ref_lvl2 == {"lvl3": "here"}
        dotdict_a.lvl1.lvl2.lvl3 = "there"
        assert dotdict_a.ref.ref_lvl2 == {"lvl3": "there"}

    def test_unknow_ref_key_error(self):
        dict_a = {"lvl1": {"lvl2": {"lvl3": "here"}}, "ref_lvl99": "$lvl1.lvl2.lvl99"}
        dotdict_a = Dotdict(dict_a)

        assert dotdict_a.ref_lvl99 is None

    def test_ref_in_sub_dict(self):
        dict_a = {"lvl1": {"lvl2": {"lvl3": "here"}}, "ref": {"ref_lvl2": "$lvl1.lvl2"}}
        dotdict_a = Dotdict(dict_a)

        sub_dict = dotdict_a.ref
        assert sub_dict.ref_lvl2 == {"lvl3": "here"}

    def test_dotdict_sublist_are_dotdict(self):
        dict_a = {"lvl1": [{"lvl2": {"lvl3": "here"}}, {"ref": {"lvl2": "there"}}]}
        dotdict_a = Dotdict(dict_a)

        assert isinstance(dotdict_a.lvl1[1], Dotdict)
