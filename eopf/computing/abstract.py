import contextlib
from abc import ABC, abstractmethod
from typing import Any

import numpy as np
from dask import array as da
from numpy.typing import DTypeLike

from eopf.product import EOProduct, EOVariable
from eopf.product.store.abstract import EOProductStore, StorageStatus
from eopf.product.store.store_factory import EOStoreFactory


class EOProcessingStep(ABC):
    _identifier: Any

    @property
    def identifier(self) -> Any:
        return self._identifier

    def __init__(self, identifier: Any = ""):
        self._identifier = identifier or id(self)

    def __call__(self, *args: EOVariable, dtype: DTypeLike = float, **kwargs: Any) -> EOVariable:
        return self.apply(*args, dtype=dtype, **kwargs)

    @abstractmethod
    def apply(self, *args: EOVariable, dtype: DTypeLike = float, **kwargs: Any) -> EOVariable:
        ...


class EOBlockProcessingStep(EOProcessingStep):
    def apply(self, *args: EOVariable, dtype: DTypeLike = float, **kwargs: Any) -> EOVariable:
        blocks = da.map_blocks(self.func, *map(lambda x: x.data, args), dtype=dtype, **kwargs)
        if not isinstance(blocks, EOVariable):
            return EOVariable(data=blocks)
        return blocks

    @abstractmethod
    def func(self, *args: np.ndarray[Any, np.dtype[Any]], **kwargs: Any) -> np.ndarray[Any, np.dtype[Any]]:
        ...


class EOOverlapProcessingStep(EOProcessingStep):
    def apply(self, *args: EOVariable, dtype: DTypeLike = float, depth: int = 1, **kwargs: Any) -> EOVariable:
        blocks = da.map_overlap(
            self.func, *map(lambda x: x.data, args), depth=depth, dtype=dtype, meta=np.array((), dtype=dtype), **kwargs
        )
        if not isinstance(blocks, EOVariable):
            return EOVariable(data=blocks)
        return blocks

    @abstractmethod
    def func(self, *args: np.ndarray[Any, np.dtype[Any]], **kwargs: Any) -> np.ndarray[Any, np.dtype[Any]]:
        ...


class EOProcessingUnit(ABC):
    _identifier: Any

    @property
    def identifier(self) -> Any:
        return self._identifier

    def instantiate_output(self, *args, **kwargs):
        return EOProduct(self.identifier, *args, **kwargs)

    def __init__(self, identifier: Any = "") -> None:
        self._identifier = identifier or id(self)

    def __call__(self, *args: EOProduct, **kwargs: Any) -> EOProduct:
        with contextlib.ExitStack() as stack:
            for product in args:
                if product.store is not None and product.store.status != StorageStatus.OPEN:
                    stack.enter_context(product.open(mode="r"))
            return self.run(*args, **kwargs)

    @abstractmethod
    def run(self, *args: EOProduct, **kwargs: Any) -> EOProduct:
        ...

    def __str__(self) -> str:
        return f"{self.__class__.__name__}<{self.identifier}>"

    def __repr__(self) -> str:
        return f"[{id(self)}]{str(self)}"


class EOProcessorMixin(ABC):
    def validate_product(self, product: EOProduct) -> None:
        product.validate()


class EOProcessor(EOProcessingUnit, EOProcessorMixin):
    _OUTPUT_PATH: str
    _STORE_TYPE: str = "zarr"

    @property
    def target_store(self) -> EOProductStore:
        return EOStoreFactory().get_store(self._OUTPUT_PATH, item_format=self._STORE_TYPE)

    def __call__(self, *args: EOProduct, **kwargs: Any) -> EOProduct:
        product = super().__call__(*args, **kwargs)
        self.validate_product(product)
        with product.open(store_or_path_url=self.target_store, mode="w"):
            product.write()
        return product
