from pathlib import Path
from pstats import Stats

import dask.array as da
import pytest

from eopf.exceptions import DaskProfilerError, SingleThreadProfilerError
from eopf.tracing import dask_profiler, single_thread_profiler


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
