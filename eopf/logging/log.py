from cProfile import Profile
from functools import wraps
from logging import Logger, getLogger
from logging.config import dictConfig, fileConfig
from pathlib import Path
from pstats import Stats
from time import time
from typing import Any, Callable, Union

from dask.distributed import Client, performance_report
from yaml import safe_load

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


class EOLogFactory(object):
    """A factory generating Python Logger based on given configuration

    Attributes
    ----------
    _cfgs: dict[str, Path]
        dictionary of logger configurations
    cfg_dir: Path
        path to the directory containing logger configurations
    """

    def __init__(self) -> None:
        """Initializes by registering logger configurations in the cfg_dir

        Raises
        ----------
        LoggingConfigurationDirDoesNotExist
            When the preset or given logging directory does not exist
        """

        self._cfgs: dict[str, Path] = {}
        self.cfg_dir = Path(__file__).parent / "conf"
        if not self.cfg_dir.is_dir():
            raise LoggingConfigurationDirDoesNotExist("The logging configuration directory must exist")
        self.register_cfg_dir()

    def __new__(cls) -> "EOLogFactory":
        """Ensures there is only one object of EOLogFactory (singletone)"""

        if not hasattr(cls, 'instance'):
            cls.instance = super(EOLogFactory, cls).__new__(cls)
        return cls.instance

    def get_cfg_path(self, cfg_name: str) -> Path:
        """Retrieve a logger configuration path by its name

        Parameters
        ----------
        cfg_name: str
            name of the logger configuration

        Raises
        ----------
        LoggingConfigurationNotRegistered
            When a given logging configuration name is not registered
        """

        if cfg_name not in self._cfgs:
            raise LoggingConfigurationNotRegistered(f"No log configuration {cfg_name} is registered")
        return self._cfgs[cfg_name]

    def set_cfg_dir(self, url: Union[str, Path] = Path(__file__).parent / "conf") -> None:
        """Set the cfg_dir parameter, remove old configurations and add those in the new cfg_dir

        Parameters
        ----------
        url: Union[str, Path]
            path to a directory containing logger configurations

        Raises
        ----------
        LoggingConfigurationDirDoesNotExist
            When the preset or given logging directory does not exist
        """
        if not isinstance(url, Path):
            self.cfg_dir = Path(url)
        else:
            self.cfg_dir = url
        if not self.cfg_dir.is_dir():
            raise LoggingConfigurationDirDoesNotExist("The logging configuration directory must exist")
        self.register_cfg_dir()

    def get_log(
            self,
            cfg_name: str = "default",
            name: str = "default",
    ) -> Logger:
        """Retrieve a logger by specifyng the name of the configuration
        and set the logger's name

        Parameters
        ----------
        cfg_name: str
            name of the logger configuration

        name: str
            name of the logger

        Raises
        ----------
        LoggingConfigurationFileIsNotValid
            When a given logging configuration file .conf/.yaml can applyed
        """

        # retrive  the cfg file path based on the name
        cfg_path = self.get_cfg_path(cfg_name)

        # load configuration file
        try:
            if cfg_path.suffix == ".conf":
                fileConfig(cfg_path, disable_existing_loggers=False)
            else:
                # yaml configuration
                with open(cfg_path, 'r') as f:
                    yaml_cfg = safe_load(f.read())
                    dictConfig(yaml_cfg)
        except Exception as e:
            raise LoggingConfigurationFileIsNotValid(f"Invalid configuration file, reason: {e}")

        return getLogger(name)

    def register_cfg(self, cfg_name: str, cfg_path: Union[Path, str]) -> None:
        """Register a logger configuration by name and path

        Parameters
        ----------
        cfg_name: str
            name of the logger configuration

        cfg_path: Union[Path, str]
            path of the logger configuration

        Raises
        ----------
        LoggingConfigurationAlreadyExists
            When a logging configuration with the same name is already registered
        FileNotFoundError
            When a file is not found at given location
        LoggingConfigurationFileTypeNotSupported
            When the logging file name does not have a .conf or .yaml extension
        """

        if not isinstance(cfg_path, Path):
            cfg_file_path = Path(cfg_path)
        else:
            cfg_file_path = cfg_path
        if cfg_name in self._cfgs:
            raise LoggingConfigurationAlreadyExists(f"A configuration file name {cfg_name} is already registered")
        if not cfg_file_path.is_file():
            raise FileNotFoundError(f"File {cfg_file_path} can not be found")
        if cfg_file_path.suffix not in [".conf", ".yaml"]:
            raise LoggingConfigurationFileTypeNotSupported("Unsuported configuration file type")
        self._cfgs[cfg_name] = cfg_file_path

    def register_cfg_dir(self) -> None:
        """Removes current logger configurations and registers new configurations from cfg_dir

        Raises
        ----------
        NoLoggingConfigurationFile
            When the preset/given logging configuration file does not contain any .conf or yaml file
        """

        # remove current configurations
        self._cfgs = {}

        # register the configurations in the cfg_dir
        no_cofiguration_present = True
        for cfg_path in self.cfg_dir.glob(r"*"):
            if cfg_path.is_file() and cfg_path.suffix in [".conf", ".yaml"]:
                no_cofiguration_present = False
                self.register_cfg(cfg_path.stem, cfg_path)

        if no_cofiguration_present:
            raise NoLoggingConfigurationFile(f"No logging configuration file .conf/.yaml is present in {self.cfg_dir}")


def _in_ipynb() -> bool:
    """Determines if the code calling the function is run in IPython

    Returns
    ----------
    bool    True if code is run in IPython interative shell, False otherwise
    """
    try:
        from IPython import get_ipython
        if 'IPKernelApp' not in get_ipython().config:  # pragma: no cover
            return False
    except ImportError:
        return False
    except AttributeError:
        return False
    return True


def dask_profiler(
    n_workers: int = None,
    threads_per_worker: int = None,
    report_name: str = "dask-report.html",
    display_report: bool = True,
) -> Any:
    """Wrapper function used to perform multi-threaded profiling (dask) on code running in dask

    Parameters
    ----------
    n_workers: int
        number of workers to be run with dask
    threads_per_worker: int
        number of threads per dask worker
    report_name: str
        name of dask html generated report
    display_report: bool
        whether to display the report as embeded html in interative console such as IPython

    Raises
    ----------
    DaskProfilerError
        When the dask_profiler raises any error
    DaskProfilerHtmlDisplayNotWorking
        When the report display of the dask_profiler is not working

    Returns
    ----------
    Any: the return of the wrapped function

    Examples
    --------
    >>> @dask_profiler(n_workers=4, threads_per_worker=2, report_name="experiment1.html", display=False)
    >>> def conv():
    ...     safe_store = EOSafeStore("data/olci.SEN3")
    ...     nc_store = EONetCDFStore("data/olci.nc")
    ...     convert(safe_store, nc_store)
    ...
    >>> @dask_profiler()
    >>> def conv():
    ...     safe_store = EOSafeStore("data/olci.SEN3")
    ...     nc_store = EONetCDFStore("data/olci.nc")
    ...     convert(safe_store, nc_store)
    """
    def wrap_outer(fn: Callable[[Any, Any], Any]) -> Any:
        @wraps(fn)
        def wrap_inner(*args: Any, **kwargs: Any) -> Any:

            # determine if a specific dask configuration is given
            if (
                n_workers and
                    n_workers > 0 and
                    threads_per_worker and
                    threads_per_worker > 0
            ):
                dask_specific_config = True
            else:
                dask_specific_config = False

            try:
                # start a dask cluster with given specification
                if dask_specific_config:
                    client = Client(
                        n_workers=n_workers,
                        threads_per_worker=threads_per_worker,
                    )

                # wrap the actual execution of dask function with perfomance monitoring
                start_time = time()
                with performance_report(filename=report_name):
                    result = fn(*args, **kwargs)
                end_time = time()
                elapsed_time = end_time - start_time
                print(f"The execution took {elapsed_time}")

                # close the client to avoid hogging ports
                if dask_specific_config:
                    client.close()

            except Exception as e:
                raise DaskProfilerError(f"Exception encountered: {e}")

            # display the html report if wanted and possible
            if display_report and _in_ipynb():
                from IPython.display import HTML, display
                try:
                    with open(report_name, mode="r") as f:
                        html_page = f.read()
                    display(HTML(html_page))
                except Exception as e:
                    raise DaskProfilerHtmlDisplayNotWorking(f"Report display faillure reason: {e}")

            return result
        return wrap_inner
    return wrap_outer


def single_thread_profiler(
    report_name: str = None,
) -> Any:
    """Decorator function used to perform single threaded profiling (cProfile) on code running in dask

    Parameters
    ----------
    report_name: str
        of cProfile.Stats dump file

    Raises
    ----------
    SingleThreadProfilerError
        When the single_thread_profiler raises any error

    Returns
    ----------
    Any: a pstat.Stats object ordered by n_calls

    Examples
    --------
    >>> @single_thread_profiler("stats.dump")
    >>> def conv():
    ...     safe_store = EOSafeStore("data/olci.SEN3")
    ...     nc_store = EONetCDFStore("data/olci.nc")
    ...     convert(safe_store, nc_store)
    ...
    >>> stats = conv()
    >>> stats.stats.strip_dirs().print_stats()

    Can be used together with dask_reporter decorator as long as it does not set a specific dask configuration:
    >>> @single_thread_profiler("stats.dump")
    >>> @dask_profiler(report_name="dask_out.html")
    >>> def conv():
    ...     safe_store = EOSafeStore("data/olci.SEN3")
    ...     nc_store = EONetCDFStore("data/olci.nc")
    ...     convert(safe_store, nc_store)

    Notes
    -----
    In Ipython environmnets one can use snakeviz to obtain a graphical representation of the returned Stats:
    >>> pip install snakeviz
    >>> %load_ext snakeviz
    >>> %snakeviz stats

    See Also
    -------
    pstats.Stats
    """
    def wrap_outer(fn: Callable[[Any, Any], Any]) -> Any:
        @wraps(fn)
        def wrap_inner(*args: Any, **kwargs: Any) -> Stats:
            try:
                # start a dask cluster with 1 worker and 1 thread
                client = Client(
                    n_workers=1,
                    threads_per_worker=1,
                )

                # wrap the actual execution of dask function with perfomance monitoring
                profiler = Profile()
                profiler.enable()
                fn(*args, **kwargs)
                profiler.disable()

                # extract, print and return stats
                stats: Stats = Stats(profiler).sort_stats('ncalls')
                if report_name:
                    stats.dump_stats(report_name)
                stats.print_stats()

                # close the client to avoid hogging ports
                client.close()

                return stats

            except Exception as e:
                raise SingleThreadProfilerError(f"Exception encountered: {e}")

        return wrap_inner
    return wrap_outer
