from typing import Sequence

from eopf.computing.abstract import ProcessingUnit
from eopf.computing.general import ChainProcessor

from .unit import OlciL2FinalisingUnit, OlciL2LandUnit, OlciL2PreProcessingUnit


class OlciL2LandProcessor(ChainProcessor):

    _PROCESSES: Sequence[ProcessingUnit] = [
        OlciL2PreProcessingUnit(identifier="olci-l2-pre-processing"),
        OlciL2LandUnit(identifier="olci-l2-unit"),
        OlciL2FinalisingUnit(identifier="olci-l2-finalize"),
    ]
