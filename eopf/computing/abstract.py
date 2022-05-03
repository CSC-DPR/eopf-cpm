from abc import ABC, abstractmethod
from typing import Any, Protocol

import numpy as np
from dask import array as da
from numpy.typing import DTypeLike

from eopf.product import EOProduct, EOVariable
from eopf.product.store.store_factory import EOStoreFactory


class ProcessingStep(Protocol):
    _identifier: Any

    @property
    def identifier(self) -> Any:
        return self._identifier

    def __init__(self, identifier: Any = ""):
        self._identifier = identifier or id(self)

    def __call__(self, *args: EOVariable, dtype: DTypeLike = float, **kwargs: Any) -> Any:
        return self.apply(*args, dtype=dtype, **kwargs)

    @abstractmethod
    def apply(self, *args: EOVariable, dtype: DTypeLike = float, **kwargs: Any) -> EOVariable:
        ...


class BlockProcessingStep(ProcessingStep):
    def apply(self, *args: EOVariable, dtype: DTypeLike = float, **kwargs: Any) -> EOVariable:
        blocks = da.map_blocks(self.func, *args, dtype=dtype, **kwargs)
        if not isinstance(blocks, EOVariable):
            return EOVariable(data=blocks)
        return blocks

    @abstractmethod
    def func(self, *args: np.ndarray[Any, np.dtype[Any]], **kwargs: Any) -> np.ndarray[Any, np.dtype[Any]]:
        ...


class OverlapProcessingStep(ProcessingStep):
    def apply(self, *args: EOVariable, dtype: DTypeLike = float, depth: int = 1, **kwargs: Any) -> EOVariable:
        blocks = da.map_overlap(self.func, *args, depth=depth, dtype=dtype, meta=np.array((), dtype=dtype), **kwargs)
        if not isinstance(blocks, EOVariable):
            return EOVariable(data=blocks)
        return blocks

    @abstractmethod
    def func(self, *args: np.ndarray[Any, np.dtype[Any]], **kwargs: Any) -> np.ndarray[Any, np.dtype[Any]]:
        ...


class ProcessingUnit(Protocol):
    _identifier: Any
    _ouput_path: str
    _STORE_TYPE: str = "zarr"

    @property
    def identifier(self) -> Any:
        return self._identifier

    def __init__(self, identifier: Any = ""):
        self._identifier = identifier or id(self)

    def __call__(self, *args: EOProduct, **kwargs: Any) -> EOProduct:
        return self.run(*args, **kwargs)

    @abstractmethod
    def run(self, *args: EOProduct, **kwargs: Any) -> EOProduct:
        ...

    @property
    def target_store(self):
        return EOStoreFactory().get_store(self._STORE_TYPE)


class ProcessorMixin(ABC):
    def validate_product(self, product: EOProduct) -> None:
        product.validate()


class Processor(ProcessingUnit, ProcessorMixin):
    def __call__(self, *args: EOProduct, **kwargs: Any) -> Any:
        product = self.run(*args, **kwargs)
        self.validate_product(product)
        return product
