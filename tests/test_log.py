from logging import Logger
from pathlib import Path

import pytest

from eopf.exceptions import (
    LoggingConfigurationAlreadyExists,
    LoggingConfigurationDirDoesNotExist,
    LoggingConfigurationFileIsNotValid,
    LoggingConfigurationFileTypeNotSupported,
    LoggingConfigurationNotRegistered,
)
from eopf.exceptions.warnings import NoLoggingConfigurationFile
from eopf.logging import EOLogFactory


@pytest.mark.unit
def test_correct_init():
    """Test that the EOLogFactory is correctly instiated"""
    test_factory = EOLogFactory()
    assert isinstance(test_factory, EOLogFactory)


@pytest.mark.unit
def test_correct_return_conf():
    """Test that the EOLogFactory returns a Logger object configured through a yaml file"""
    test_factory = EOLogFactory()
    test_log = test_factory.get_log()
    assert isinstance(test_log, Logger)


@pytest.mark.unit
def test_correct_return_yaml():
    """Test that the EOLogFactory returns a Logger object configured through a conf file"""
    test_factory = EOLogFactory()
    yaml_path = Path(__file__).parent / "data" / "a_yaml_log_conf.yaml"
    test_factory.register_cfg("a_yaml", yaml_path)
    test_log = test_factory.get_log("a_yaml")
    assert isinstance(test_log, Logger)


@pytest.mark.unit
def test_unregistered_configuration():
    """Test when trying to get log without a registered configuration it raises LoggingConfigurationNotRegistered"""
    test_factory = EOLogFactory()
    with pytest.raises(LoggingConfigurationNotRegistered):
        _ = test_factory.get_log(cfg_name="default2")


@pytest.mark.unit
def test_non_existent_cfg_dir():
    """Test that when setting the cfg dir with a non-existent dir it raises LoggingConfigurationDirDoesNotExist"""
    initial_conf_path = EOLogFactory().cfg_dir
    test_factory = EOLogFactory()
    with pytest.raises(LoggingConfigurationDirDoesNotExist):
        _ = test_factory.set_cfg_dir("/tmp/does_not_exist")

    # reset the path to avoid impacting other tests
    _ = test_factory.set_cfg_dir(initial_conf_path)
    


@pytest.mark.unit
def test_singletone():
    """Test that the EOLogFactory is a singletone"""
    test_factory_1 = EOLogFactory()
    test_factory_2 = EOLogFactory()
    assert id(test_factory_1) == id(test_factory_2)


@pytest.mark.unit
def test_register_duplicate_cfg():
    """Test that when setting the cfg dir with a non-existent dir it raises LoggingConfigurationDirDoesNotExist"""
    test_factory = EOLogFactory()
    with pytest.raises(LoggingConfigurationAlreadyExists):
        _ = test_factory.register_cfg("default", "")


@pytest.mark.unit
def test_register_cfg_with_incorrect_file_path():
    """Test that when setting the cfg dir with a non-existent file it raises LoggingConfigurationDirDoesNotExist"""
    test_factory = EOLogFactory()
    with pytest.raises(FileNotFoundError):
        _ = test_factory.register_cfg("default2", "")


@pytest.mark.unit
def test_register_cfg_with_incorrect_file_extension():
    """Test that when setting the cfg dir with a non-existent dir it raises LoggingConfigurationDirDoesNotExist"""

    test_factory = EOLogFactory()
    test_file_path = Path(__file__).parent / "data" / "log_conf.yam"
    with pytest.raises(LoggingConfigurationFileTypeNotSupported):
        _ = test_factory.register_cfg("invalid", cfg_path=test_file_path)


@pytest.mark.unit
def test_register_cfg_dir_with_no_log_configurations():
    """Test that when setting the cfg dir with a non-existent dir it raises NoLoggingConfigurationFile"""

    initial_conf_path = EOLogFactory().cfg_dir
    test_factory = EOLogFactory()
    test_dir_path = Path(__file__).parent
    with pytest.raises(NoLoggingConfigurationFile):
        test_factory.set_cfg_dir(test_dir_path)

    # reset the path to avoid impacting other tests
    _ = test_factory.set_cfg_dir(initial_conf_path)


@pytest.mark.unit
def test_get_log_with_non_valid_configuration_file():
    """Test that when setting the cfg dir with a non-existent dir it raises LoggingConfigurationDirDoesNotExist"""

    test_factory = EOLogFactory()
    test_file_path = Path(__file__).parent / "data" / "log_conf.conf"
    test_factory.register_cfg("invalid", cfg_path=test_file_path)
    with pytest.raises(LoggingConfigurationFileIsNotValid):
        _ = test_factory.get_log("invalid")
