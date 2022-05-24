from logging import Logger
from pathlib import Path
from pstats import Stats

import dask.array as da
import pytest

from eopf.exceptions import (
    DaskProfilerError,
    LoggingConfigurationAlreadyExists,
    LoggingConfigurationDirDoesNotExist,
    LoggingConfigurationFileIsNotValid,
    LoggingConfigurationFileTypeNotSupported,
    LoggingConfigurationNotRegistered,
    SingleThreadProfilerError,
)
from eopf.exceptions.warnings import NoLoggingConfigurationFile
from eopf.logging import EOLogFactory, dask_profiler
from eopf.logging.log import single_thread_profiler


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

    test_factory = EOLogFactory()
    test_file_path = Path(__file__).parent / "data" / "log_conf.yam"
    with pytest.raises(LoggingConfigurationFileTypeNotSupported):
        _ = test_factory.register_cfg("invalid", cfg_path=test_file_path)


@pytest.mark.unit
def test_register_cfg_dir_with_no_log_configurations():
    """Test that when setting the cfg dir with a non-existent dir it raises NoLoggingConfigurationFile"""

    test_factory = EOLogFactory()
    test_dir_path = Path(__file__).parent
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


@pytest.mark.unit
def test_dask_profiler_nominal(OUTPUT_DIR):
    expected_return_value = 100
    expected_parameter_value = 2
    report_path = Path(OUTPUT_DIR) / "test-dask-report.html"

    @dask_profiler(
        n_workers=3,
        threads_per_worker=1,
        report_name=report_path,
        display_report=False,
    )
    def just_a_simple_dask_func(a_parameter: int):
        # test parameters are passed correctly
        assert isinstance(a_parameter, int) and a_parameter == expected_parameter_value

        # some dask computation
        x = da.arange(10, chunks=10)
        x.sum().compute()

        return expected_return_value

    # test that the decorated function returns the expected value
    assert just_a_simple_dask_func(expected_parameter_value) == expected_return_value

    # test that the report was saved on disk
    assert report_path.is_file()


@pytest.mark.unit
def test_dask_profiler_raises_exception(OUTPUT_DIR):
    """Test that DaskProfilerError is raised when the decorated function raises an exception"""

    report_path = Path(OUTPUT_DIR) / "test-dask-report.html"

    @dask_profiler(
        n_workers=3,
        threads_per_worker=1,
        report_name=report_path,
        display_report=False,
    )
    def just_a_simple_dask_func():
        raise Exception("Just an exception")

    # test that an Exception is raised
    with pytest.raises(DaskProfilerError):
        _ = just_a_simple_dask_func()


@pytest.mark.unit
def test_single_threaded_profiler_nominal(OUTPUT_DIR):
    """Test nominal functioning of the single_thread_profiler"""
    expected_return_type = Stats
    expected_parameter_value = 2
    report_path = Path(OUTPUT_DIR) / "single-thread-report"

    @single_thread_profiler(
        report_name=report_path,
    )
    def just_a_simple_dask_func(a_parameter: int):
        # test parameters are passed correctly
        assert isinstance(a_parameter, int) and a_parameter == expected_parameter_value

        # some dask computation
        x = da.arange(10, chunks=10)
        x.sum().compute()

        return 100

    # test that the decorated function returns a Stats object
    assert isinstance(just_a_simple_dask_func(expected_parameter_value), expected_return_type)

    # test that the report was saved on disk
    assert report_path.is_file()


@pytest.mark.unit
def test_single_thread_profiler_raises_exception(OUTPUT_DIR):
    """Test that SingleThreadProfilerError is raised when the decorated function raises an exception"""

    @single_thread_profiler(
        report_name=Path(OUTPUT_DIR) / "single-thread-report",
    )
    def just_a_simple_dask_func():
        raise Exception("Just an exception")

    # test that an Exception is raised
    with pytest.raises(SingleThreadProfilerError):
        _ = just_a_simple_dask_func()
