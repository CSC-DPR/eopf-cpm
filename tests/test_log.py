from logging import Logger
from pathlib import Path
from tempfile import tempdir
import pytest
from eopf.logging import EOLogFactory

from eopf.exceptions import (
    DaskProfilerError,
    LoggingConfigurationAlreadyExists,
    LoggingConfigurationDirDoesNotExist,
    LoggingConfigurationFileIsNotValid,
    LoggingConfigurationFileTypeNotSupported,
    LoggingConfigurationNotRegistered,
    SingleThreadProfilerError,
)
from eopf.exceptions.warnings import (
    DaskProfilerHtmlDisplayNotWorking,
    NoLoggingConfigurationFile,
)

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
    test_log = test_factory.get_log("a_yaml")
    assert isinstance(test_log, Logger)


@pytest.mark.unit
def test_unregistered_configuration():
    """Test that when trying to get log without a registered configuration it raises LoggingConfigurationNotRegistered"""
    test_factory = EOLogFactory()
    with pytest.raises(LoggingConfigurationNotRegistered):
        _ = test_factory.get_log(cfg_name="default2")


@pytest.mark.unit
def test_non_existent_cfg_dir():
    """Test that when setting the cfg dir with a non-existent dir it raises LoggingConfigurationDirDoesNotExist"""
    test_factory = EOLogFactory()
    with pytest.raises(LoggingConfigurationDirDoesNotExist):
        _ = test_factory.set_cfg_dir("/tmp/does_not_exist")


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

    #import tempfile
    #tmp_cfg_file = tempfile.mktemp(prefix="conf", suffix=".yam")
    #tmp_cfg_file = tmpdir.mkdir("data") / "a.cof"
    test_factory = EOLogFactory()
    test_file_path = Path(__file__).parent / "data" / "log_conf.yam"
    with pytest.raises(LoggingConfigurationFileTypeNotSupported):
        _ = test_factory.register_cfg("invalid", cfg_path=test_file_path)


@pytest.mark.unit

def test_register_cfg_dir_with_no_log_configurations():
    """Test that when setting the cfg dir with a non-existent dir it raises NoLoggingConfigurationFile"""

    test_factory = EOLogFactory()
    test_dir_path = Path(__file__).parent / "data" / "empty_dir"
    with pytest.raises(NoLoggingConfigurationFile):
        test_factory.set_cfg_dir(test_dir_path)
        

@pytest.mark.unit
def test_get_log_with_non_valid_configuration_file():
    """Test that when setting the cfg dir with a non-existent dir it raises LoggingConfigurationDirDoesNotExist"""

    test_factory = EOLogFactory()
    test_file_path = Path(__file__).parent / "data" / "log_conf.conf"
    test_factory.register_cfg("invalid", cfg_path=test_file_path)
    with pytest.raises(LoggingConfigurationFileIsNotValid):
        _ = test_factory.get_log("invalid")
    
    
    
    
    
    
    


    
    
    


    
    

