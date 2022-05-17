from abc import ABC, abstractmethod
from typing import Any

import numpy as np
from dask import array as da
from numpy.typing import DTypeLike

from eopf.product import EOProduct, EOVariable


class EOProcessingStep(ABC):
    _identifier: Any

    @property
    def identifier(self) -> Any:
        return self._identifier

    def __init__(self, identifier: Any = ""):
        self._identifier = identifier or id(self)

    @abstractmethod
    def apply(self, *args: da.Array, dtype: DTypeLike = float, **kwargs: Any) -> EOVariable:  # pragma: no cover
        ...


class EOBlockProcessingStep(EOProcessingStep):
    def apply(self, *args: da.Array, dtype: DTypeLike = float, **kwargs: Any) -> da.Array:
        return da.map_blocks(self.func, *args, dtype=dtype, **kwargs)

    @abstractmethod
    def func(
        self, *args: np.ndarray[Any, np.dtype[Any]], **kwargs: Any
    ) -> np.ndarray[Any, np.dtype[Any]]:  # pragma: no cover
        ...


class EOOverlapProcessingStep(EOProcessingStep):
    def apply(self, *args: da.Array, dtype: DTypeLike = float, depth: int = 1, **kwargs: Any) -> da.Array:
        return da.map_overlap(self.func, *args, depth=depth, dtype=dtype, meta=np.array((), dtype=dtype), **kwargs)

    @abstractmethod
    def func(
        self, *args: np.ndarray[Any, np.dtype[Any]], **kwargs: Any
    ) -> np.ndarray[Any, np.dtype[Any]]:  # pragma: no cover
        ...


class EOProcessingUnit(ABC):
    _identifier: Any

    @property
    def identifier(self) -> Any:
        return self._identifier

    def __init__(self, identifier: Any = "") -> None:
        self._identifier = identifier or id(self)

    @abstractmethod
    def run(self, *args: EOProduct, **kwargs: Any) -> EOProduct:  # pragma: no cover
        ...

    def __str__(self) -> str:
        return f"{self.__class__.__name__}<{self.identifier}>"

    def __repr__(self) -> str:
        return f"[{id(self)}]{str(self)}"


class EOProcessor(EOProcessingUnit):
    def validate_product(self, product: EOProduct) -> None:
        product.validate()
