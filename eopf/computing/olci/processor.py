from typing import Sequence
from uuid import uuid4

from eopf.computing.abstract import EOProcessingUnit
from eopf.computing.general import EOChainProcessor

from .unit import OlciL2FinalisingUnit, OlciL2LandUnit, OlciL2PreProcessingUnit


class OlciL2LandProcessor(EOChainProcessor):
    _OUTPUT_PATH = f"S3_OL_2_EFR__{uuid4().hex}.zarr"

    _PROCESSES: Sequence[EOProcessingUnit] = [
        OlciL2PreProcessingUnit(identifier="olci-l2-pre-processing"),
        OlciL2LandUnit(identifier="olci-l2-unit"),
        OlciL2FinalisingUnit(identifier="olci-l2-finalize"),
    ]
