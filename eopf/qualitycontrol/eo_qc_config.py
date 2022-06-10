import glob
import json
import os
from collections.abc import MutableMapping
from typing import Iterator

from eopf.qualitycontrol.eo_qc import (
    EOQC,
    EOQCFormula,
    EOQCProcessingUnit,
    EOQCValidRange,
)


class EOQCConfig(MutableMapping):
    def __init__(self, config_path: str) -> None:
        super().__init__()
        self._qclist = {}
        self.default = False
        with open(config_path, "r") as f:
            qc_config = json.load(f)
            self.default = qc_config["default"]
            self.product_type = qc_config["product_type"]
            self.id = qc_config["id"]
            for qc_type in qc_config["quality_checks"]:
                for qc in qc_config["quality_checks"][qc_type]:
                    if qc_type == "formulas":
                        self._qclist[qc["check_id"]] = EOQCFormula(qc)
                    elif qc_type == "valid_ranges":
                        self._qclist[qc["check_id"]] = EOQCValidRange(qc)
                    else:
                        self._qclist[qc["check_id"]] = EOQCProcessingUnit(qc)

    def __getitem__(self, check_id: str) -> EOQC:
        return self._qclist[check_id]

    def __setitem__(self, check_id: str, qc: EOQC) -> None:
        self._qclist[check_id] = qc

    def __delitem__(self, check_id: str) -> None:
        return super().__delitem__(check_id)

    def __iter__(self) -> Iterator[EOQC]:
        pass

    def rm_qc(self, check_id) -> None:
        self.__delitem__(check_id)

    def __len__(self) -> int:
        return len(self._qclist)


class EOPCConfigFactory:
    def __init__(self) -> None:
        self._configs = {}
        dir_path = os.path.dirname(os.path.realpath(__file__))
        qc_configs_paths = glob.glob(f"{dir_path}/configs/*.json")
        for path_to_config in qc_configs_paths:
            qc_config = EOQCConfig(path_to_config)
            self.add_qc_config(qc_config.id, qc_config)

    def add_qc_config(self, id: str, config: EOQCConfig):
        self._configs[id] = config

    def get_qc_configs(self, product_type):
        configs = []
        for config in self._configs.values():
            if config.product_type == product_type:
                configs.append(config)
        return config

    def get_default(self, product_type):
        for config in self._configs.values():
            if config.default and config.product_type == product_type:
                return config

    def get_config_by_id(self, id):
        return self._configs[id]
