import configparser
import enum
import os
from functools import cache
from typing import Any, Mapping

import toml


class ConfigFileType(enum.Enum):
    TOML = "toml"
    INI = "ini"


class EOPFConfiguration:
    """Store and manage current configurations folders

    You can use different way to manage settings:

        - using ini file with ``eopf`` section
        - using pyproject.toml with ``tool.eopf`` section
        - using environment variable setting the value in upper case
          and prefixing it by ``EOPF_``
          (example: ``configuration_folder`` => ``EOPF_CONFIGURATION_FOLDER``)

    You can configure:

        - configuration_folder: folder containing all modules configuration
          for ``qualitycontrol`` or ``logging`` for example.

    Parameters
    ----------
    config_file: str, optional
        path to ``.ini`` file that store settings
    """

    def __init__(self, config_file: str = "") -> None:
        self.configuration_folder = os.path.expanduser(os.path.join("~", ".eopf"))
        self.load(config_file=config_file)
        if not os.path.isdir(self.configuration_folder):
            self.setup()

    @property
    def config_files(self) -> tuple[tuple[str, ConfigFileType], ...]:
        """supported default configuration file"""
        return (
            (".eopf.ini", ConfigFileType.INI),
            ("setup.cfg", ConfigFileType.INI),
            ("pyproject.toml", ConfigFileType.TOML),
        )

    @property
    def configurable_modules(self) -> tuple[str, ...]:
        """modules that can have a configuration"""
        return (self.qualitycontrol, self.logging, self.mapping)

    @property
    def configurable_values(self) -> tuple[str, ...]:
        """name of the configurable values"""
        return ("configuration-folder",)

    @property
    def logging(self) -> str:
        """path of the folder to configure logging"""
        return os.path.join(self.configuration_folder, "logging")

    @property
    def mapping(self) -> str:
        """path of the folder to configure mapping"""
        return os.path.join(self.configuration_folder, "mapping")

    @property
    def qualitycontrol(self) -> str:
        """path of the folder to configure qualitycontrol"""
        return os.path.join(self.configuration_folder, "qualitycontrol")

    def load(self, config_file: str = "") -> None:
        """Parse and load the given ``.ini`` configuration file.

        If not file is given, try to retrieve one configuration
        over the following possible file:

            - .eopf.ini
            - setup.cfg
            - pyproject.toml

        In the case that there is no configuration, defaults values
        are used.

        Parameters
        ----------
        config_files: str, optional
            path to ``.ini`` file that store settings
        """
        loader = {
            ConfigFileType.TOML: self._load_toml,
            ConfigFileType.INI: self._load_ini,
        }
        self._load_environ()
        if not config_file:
            for filename, kind in self.config_files:
                fullpath_file = os.path.join(os.path.curdir, filename)
                if os.path.isfile(fullpath_file):
                    loader[kind](fullpath_file)
        else:
            self._load_ini(config_file)

    def _load_dict(self, entry_dict: Mapping[str, Any]) -> None:
        for config_value in self.configurable_values:
            if config_value in entry_dict:
                setattr(self, config_value.replace("-", "_"), entry_dict[config_value])

    def _load_environ(self) -> None:
        self._load_dict(
            dict((env_var[5:].lower(), value) for env_var, value in os.environ.items() if env_var.startswith("EOPF_")),
        )

    def _load_ini(self, filename: str) -> None:
        base_config_data = configparser.ConfigParser()
        base_config_data.read(filenames=[filename])
        self._load_dict(base_config_data["eopf"])

    def _load_toml(self, filename: str) -> None:
        with open(filename) as f:
            base_config_data = toml.load(f)
        base_config_data = base_config_data.get("tool", {}).get("eopf", {})
        self._load_dict(base_config_data)

    def setup(self) -> None:
        """Generate all folder hierarchy's for each configuration
        per module
        """
        os.makedirs(self.configuration_folder)
        for module in self.configurable_modules:
            os.mkdir(module)


@cache
def conf_loader(config_file: str = "") -> EOPFConfiguration:
    """Cached configuration loader

    Parameters
    ----------
    config_file: str, optional
        path to ``.ini`` file that store settings
    """
    return EOPFConfiguration(config_file=config_file)
