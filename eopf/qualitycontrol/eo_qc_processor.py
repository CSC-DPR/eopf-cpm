from typing import Any, Dict, Optional

from eopf.computing.abstract import EOProcessor
from eopf.product.core.eo_product import EOProduct
from eopf.qualitycontrol.eo_qc_config import EOPCConfigFactory, EOQCConfig


class EOQCProcessor(EOProcessor):
    def __init__(self, eoproduct: Optional[EOProduct] = None, qc_list: Optional[EOQCConfig] = []) -> None:
        self.qc_config = None
        self.eoproduct = eoproduct
        self.qc_configfactory = EOPCConfigFactory()
        if len(qc_list) > 0:
            for qc in qc_list:
                self.qc_configfactory.add_qc_config(qc)

    def run(self, eoproduct: EOProduct, parameters: Dict[str, Any]) -> EOProduct:
        self.eoproduct = eoproduct
        self.qc_config = self.qc_configfactory.get_default(eoproduct.type)
        for qc in self.qc_config._qclist.values():
            qc.check(self.eoproduct)
        if "quality" not in eoproduct:
            self.eoproduct.add_group("quality")
        if "qc" not in self.eoproduct.quality:
            self.eoproduct.quality.attrs["qc"] = {}
        for qc in self.qc_config._qclist.values():
            self.eoproduct.quality.attrs["qc"][qc.id] = {"version": qc.version, "status": bool(qc.status)}
        return self.eoproduct

    def write_report(self, str) -> bool:
        pass
