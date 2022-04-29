from typing import Any

import numpy as np

from eopf.computing.abstract import BlockProcessingStep


class NdviStep(BlockProcessingStep):
    def func(  # type: ignore[override]
        self,
        ir: np.ndarray[Any, np.dtype[Any]],
        red: np.ndarray[Any, np.dtype[Any]],
        mask: np.ndarray[Any, np.dtype[Any]] = None,
    ) -> np.ndarray[Any, np.dtype[Any]]:
        if mask is not None:
            ir = np.ma.array(ir, mask=mask)
            red = np.ma.array(red, mask=mask)
        return (ir - red) / (ir + red)
