import glob
import json
import os
from collections import defaultdict
from collections.abc import MutableMapping
from typing import Iterator

from eopf.exceptions import (
    EOPCConfigFactoryAlreadyDefaultConfiguration,
    EOPCConfigFactoryNoDefaultConfiguration,
)
from eopf.qualitycontrol.eo_qc import (
    EOQC,
    EOQCFormula,
    EOQCProcessingUnit,
    EOQCValidRange,
)


class EOQCConfig(MutableMapping[str, EOQC]):
    """Quality control configuration. It contain one or multiple quality check.

    Parameters
    ----------
    config_path: str
        The path to the .json configuration file.

    Attributes
    ----------
    _qclist: list[EOQC]
        The list of the quality check.
    default : bool
        Is this quality control configuration the default one ?
    """

    quality_type = defaultdict(lambda: EOQCProcessingUnit, formulas=EOQCFormula, valid_ranges=EOQCValidRange)

    def __init__(self, config_path: str) -> None:
        super().__init__()
        self._qclist = {}
        self.default = False
        with open(config_path, "r") as f:
            qc_config = json.load(f)
            self.id = qc_config["id"]
            self.default = qc_config["default"]
            self.product_type = qc_config["product_type"]
            if qc_config.get("common_qc", False) is not False:
                common_qc_paths = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/common_qc.json")
                with open(common_qc_paths, "r") as w:
                    common_qc = json.load(w)
                    for qc_type in common_qc["quality_checks"]:
                        for qc in common_qc["quality_checks"][qc_type]:
                            self._qclist[qc["check_id"]] = self.quality_type[qc_type](**qc)
            for qc_type in qc_config["quality_checks"]:
                for qc in qc_config["quality_checks"][qc_type]:
                    self._qclist[qc["check_id"]] = self.quality_type[qc_type](**qc)

    @property
    def qclist(self) -> dict[str, EOQC]:
        """quality check list"""
        return self._qclist

    def __getitem__(self, check_id: str) -> EOQC:
        return self._qclist[check_id]

    def __setitem__(self, check_id: str, qc: EOQC) -> None:
        self._qclist[check_id] = qc

    def __delitem__(self, check_id: str) -> None:
        return self._qclist.__delitem__(check_id)

    def __iter__(self) -> Iterator[str]:
        return iter(self._qclist)

    def rm_qc(self, check_id: str) -> None:
        """Remove a check of the quality control configuration.

        Parameters
        ----------
        check_id: str
            ID of the check to remove.
        """
        self.__delitem__(check_id)

    def __len__(self) -> int:
        return len(self._qclist)


class EOPCConfigFactory:
    """Quality control configuration factory. It contain one or multiple quality control configuration.

    Attributes
    ----------
    _configs: list[EOQCConfig]
        The list of quality control configuration.
    """

    def __init__(self) -> None:
        self._configs: dict[str, EOQCConfig] = {}
        dir_path = os.path.dirname(os.path.realpath(__file__))
        qc_configs_paths = glob.glob(f"{dir_path}/configs/*.json")
        for path_to_config in qc_configs_paths:
            if not os.path.basename(path_to_config) == "common_qc.json":
                qc_config = EOQCConfig(path_to_config)
                self.add_qc_config(qc_config.id, qc_config)

    def add_qc_config(self, id: str, config: EOQCConfig) -> None:
        """Add a quality control configuration to the quality control configuration factory.
        If the configuration to add is a default one, it check that their is not already one for this product type.

        Parameters
        ----------
        id: str
            ID of the config to add.
        config: EOQCConfig
            The config to add.

        Raises
        ------
        EOPCConfigFactoryAlreadyDefaultConfiguration
            Raise this error when their is already a default configuration for this product type.
        """
        # Check if their is not another default configuration for this product
        if config.default:
            configs_wpt = self.get_qc_configs(config.product_type)
            for cfg in configs_wpt:
                if cfg.default:
                    raise EOPCConfigFactoryAlreadyDefaultConfiguration(
                        f"Product type : {config.product_type} already have a default configuration",
                    )
        # If ok then add it to the configs
        self._configs[id] = config

    def get_qc_configs(self, product_type: str) -> list[EOQCConfig]:
        """Get all the quality control configuration for a specific product type.

        Parameters
        ----------
        product_type: str
            The product type.

        Returns
        -------
        list[EOQCConfig]
            The list of quality control configuration for the parameter product type.
        """
        return [config for config in self._configs.values() if config.product_type == product_type]

    def get_default(self, product_type: str) -> EOQCConfig:
        """Get the default quality control configuration for a specific product type.

        Parameters
        ----------
        product_type: str
            The product type.

        Returns
        -------
        EOQCConfig
            The default quality control configuration for the parameter product type.

        Raises
        ------
        EOPCConfigFactoryNoDefaultConfiguration
            Raise this error when their is no default configuration for this product type.
        """
        for config in self._configs.values():
            if config.default and config.product_type == product_type:
                return config
        raise EOPCConfigFactoryNoDefaultConfiguration(
            f"No default configuration found for product type : {product_type}",
        )

    def get_config_by_id(self, id: str) -> EOQCConfig:
        """Get a quality control configuration with the id.

        Parameters
        ----------
        id: str
            The product id.

        Returns
        -------
        EOQCConfig
            The quality control configuration matching the id parameter.
        """
        return self._configs[id]
