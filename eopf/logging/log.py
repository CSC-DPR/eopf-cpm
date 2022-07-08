from json import load
from logging import CRITICAL, DEBUG, ERROR, INFO, NOTSET, WARNING, Logger, getLogger
from logging.config import dictConfig
from pathlib import Path
from typing import Union

from eopf.exceptions import (
    LoggingConfigurationAlreadyExists,
    LoggingConfigurationDirDoesNotExist,
    LoggingConfigurationFileIsNotValid,
    LoggingConfigurationFileTypeNotSupported,
    LoggingConfigurationNotRegistered,
)
from eopf.exceptions.warnings import (
    LoggingLevelIsNoneStandard,
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

    cfg_dir = Path(__file__).parent / "conf"
    _cfgs: dict[str, Path] = {}

    def __init__(self) -> None:
        """Initializes by registering logger configurations in the cfg_dir

        Raises
        ----------
        LoggingConfigurationDirDoesNotExist
            When the preset or given logging directory does not exist
        """
        if not self.cfg_dir.is_dir():
            raise LoggingConfigurationDirDoesNotExist("The logging configuration directory must exist")
        self.register_cfg_dir()

    def __new__(cls) -> "EOLogFactory":
        """Ensures there is only one object of EOLogFactory (singletone)"""

        if not hasattr(cls, "instance"):
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
        level: int = None,
    ) -> Logger:
        """Retrieve a logger by specifyng the name of the configuration
        and set the logger's name

        Parameters
        ----------
        cfg_name: str
            name of the logger configuration

        name: str
            name of the logger

        level: int
            logger level

        Raises
        ----------
        LoggingConfigurationFileIsNotValid
            When a given logging configuration file .conf/.yaml can applyed
        """

        # retrive  the cfg file path based on the name
        cfg_path = self.get_cfg_path(cfg_name)

        # load json configuration file
        try:
            with open(cfg_path, "r") as f:
                json_cfg = load(f)
                dictConfig(json_cfg)
        except Exception as e:
            raise LoggingConfigurationFileIsNotValid(f"Invalid configuration file, reason: {e}")

        logger = getLogger(name)

        # override the logging level from the cfg file
        if level and level != NOTSET:
            if level not in [DEBUG, INFO, WARNING, ERROR, CRITICAL]:
                raise LoggingLevelIsNoneStandard("The given log level is set to a value which is none Python standard")
            logger.setLevel(level=level)

        return logger

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
            When the logging file name does not have a .json extension
        """

        if not isinstance(cfg_path, Path):
            cfg_file_path = Path(cfg_path)
        else:
            cfg_file_path = cfg_path
        if cfg_name in self._cfgs:
            raise LoggingConfigurationAlreadyExists(f"A configuration file name {cfg_name} is already registered")
        if not cfg_file_path.is_file():
            raise FileNotFoundError(f"File {cfg_file_path} can not be found")
        if cfg_file_path.suffix not in [".json"]:
            raise LoggingConfigurationFileTypeNotSupported("Unsuported configuration file type")
        self._cfgs[cfg_name] = cfg_file_path

    def register_cfg_dir(self) -> None:
        """Removes current logger configurations and registers new configurations from cfg_dir

        Raises
        ----------
        NoLoggingConfigurationFile
            When the preset/given logging configuration file does not contain .json files
        """

        # remove current configurations
        self._cfgs = {}

        # register the configurations in the cfg_dir
        no_cofiguration_present = True
        for cfg_path in self.cfg_dir.iterdir():
            if cfg_path.is_file() and cfg_path.suffix in [".json"]:
                no_cofiguration_present = False
                self.register_cfg(cfg_path.stem, cfg_path)

        if no_cofiguration_present:
            raise NoLoggingConfigurationFile(f"No logging configuration file .json is present in {self.cfg_dir}")
