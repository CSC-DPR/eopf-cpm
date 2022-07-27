import os
from unittest import mock

import pytest

from eopf.conf import EOPFConfiguration


@pytest.fixture
def FOLDER_WITH_CONFIGS(EMBEDED_TEST_DATA_FOLDER):
    return os.path.join(EMBEDED_TEST_DATA_FOLDER, "config")


@pytest.fixture
def INI_CONF_FILE(FOLDER_WITH_CONFIGS):
    return os.path.join(FOLDER_WITH_CONFIGS, "conf.ini")


@pytest.fixture
def TOML_CONF_FILE(FOLDER_WITH_CONFIGS):
    return os.path.join(FOLDER_WITH_CONFIGS, "conf.toml")


@pytest.fixture
def CONFIGURATION_FOLDER(OUTPUT_DIR):
    return os.path.join(OUTPUT_DIR, "config")


def test_load(INI_CONF_FILE):
    with (
        mock.patch("eopf.conf.EOPFConfiguration.setup"),
        mock.patch.dict(
            os.environ,
            {
                "EOPF_CONFIGURATION_FOLDER": "environ_parsed_value",
                "EOPF_INVALID_KEY": "environ_false_value",
                "OTHERS": "not_parsed",
            },
        ),
    ):
        config = EOPFConfiguration(INI_CONF_FILE)
        assert config.configuration_folder == "ini_parsed_value"
        assert not hasattr(config, "key_should_not_be_here")
        assert all(hasattr(config, value.replace("-", "_")) for value in config.configurable_values)


def test_load_ini(INI_CONF_FILE):
    with mock.patch("eopf.conf.EOPFConfiguration.setup"):
        config = EOPFConfiguration()
        config._load_ini(INI_CONF_FILE)
        assert config.configuration_folder == "ini_parsed_value"
        assert not hasattr(config, "key_should_not_be_here")
        assert all(hasattr(config, value.replace("-", "_")) for value in config.configurable_values)


def test_load_toml(TOML_CONF_FILE):
    with mock.patch("eopf.conf.EOPFConfiguration.setup"):
        config = EOPFConfiguration()
        config._load_toml(TOML_CONF_FILE)
        assert config.configuration_folder == "toml_parsed_value"
        assert not hasattr(config, "key_should_not_be_here")
        assert all(hasattr(config, value.replace("-", "_")) for value in config.configurable_values)


def test_load_environ():
    with (
        mock.patch.dict(
            os.environ,
            {
                "EOPF_CONFIGURATION_FOLDER": "environ_parsed_value",
                "EOPF_INVALID_KEY": "environ_false_value",
                "OTHERS": "not_parsed",
            },
        ),
        mock.patch("eopf.conf.EOPFConfiguration.setup"),
    ):
        config = EOPFConfiguration()
        config._load_environ()
        assert config.configuration_folder == "environ_parsed_value"
        assert not any(hasattr(config, value) for value in ("invalid_key", "others"))
        assert all(hasattr(config, value.replace("-", "_")) for value in config.configurable_values)


def test_setup(CONFIGURATION_FOLDER):

    with mock.patch.dict(
        os.environ,
        {
            "EOPF_CONFIGURATION_FOLDER": CONFIGURATION_FOLDER,
        },
    ):
        config = EOPFConfiguration()
        assert config.configuration_folder == CONFIGURATION_FOLDER
        assert os.path.isdir(CONFIGURATION_FOLDER)
        assert all(module.startswith(CONFIGURATION_FOLDER) for module in config.configurable_modules)
