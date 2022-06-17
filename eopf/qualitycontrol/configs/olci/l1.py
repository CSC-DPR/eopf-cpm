from typing import Any, Dict

from eopf.computing.abstract import EOProcessingUnit
from eopf.product import EOProduct


class QC01Unit(EOProcessingUnit):
    def run(self, inputs: Dict[str, EOProduct], parameters: Dict[str, Any]) -> EOProduct:
        eoproduct = inputs["input"]
        eoproduct.attrs["QC01Unit"] = {"status": True}
        return eoproduct
