import pathlib
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Optional, Union

import rioxarray
import xarray

from eopf.exceptions import StoreNotOpenError
from eopf.product.store.abstract import EOProductStore

if TYPE_CHECKING:
    from distributed import Lock

    from eopf.product.core.eo_object import EOObject


class EORasterIOAccessor(EOProductStore):
    """
    Accessor representation to access Raster like jpg2000 or tiff.

    Parameters
    ----------
    url: str
        path or url to access

    Attributes
    ----------
    url: str
        path or url to access
    """

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self._ref: Optional[Any] = None
        self._mode: Optional[str] = None
        self._lock: Optional[Lock] = None

    # docstr-coverage: inherited
    @property
    def is_writeable(self) -> bool:
        return False

    # docstr-coverage: inherited
    @property
    def is_erasable(self) -> bool:
        return False

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open(mode=mode)
        self._ref = rioxarray.open_rasterio(self.url, **kwargs)
        self._mode = mode
        self._lock = kwargs.get("lock")

    # docstr-coverage: inherited
    def close(self) -> None:
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        super().close()
        if self._mode == "w":
            self._ref.rio.to_raster(
                self.url,
                tiled=True,
                lock=self._lock,
            )
        self._mode = None
        self._lock = None
        self._ref = None

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        node = self._select_node(path)
        return isinstance(node, (xarray.Dataset, list))

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        node = self._select_node(path)
        return isinstance(node, xarray.DataArray)

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        node = self._select_node(group_path)
        if isinstance(node, list):
            raise NotImplementedError()
        node.attrs.update(attrs)  # type: ignore[arg-type]

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        node = self._select_node(path)
        if isinstance(node, list):
            return iter(ds.name for ds in node)
        if isinstance(node, xarray.Dataset):
            return iter(str(i) for i in iter(node))
        return iter([])

    # docstr-coverage: inherited
    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".tiff", ".tif"]

    def _select_node(self, path: str) -> Union[list[xarray.Dataset], xarray.DataArray, xarray.Dataset]:
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current, _, sub_path = path.partition(self.sep)
        if path in ["", "/"]:
            return self._ref

        if isinstance(self._ref, list):
            for dataset in self._ref:
                if dataset.name == current:
                    if not sub_path:
                        return dataset
                    if sub_path in dataset:  # path is a datarray
                        return dataset[sub_path]
            raise KeyError()

        if path in [self._ref.name]:
            return self._ref

        if isinstance(self._ref, xarray.Dataset) and path in self._ref:
            return self._ref[path]
        raise KeyError()

    def __getitem__(self, key: str) -> "EOObject":
        from eopf.product.core.eo_group import EOGroup
        from eopf.product.core.eo_variable import EOVariable

        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        node = self._select_node(key)
        if isinstance(node, list):
            raise NotImplementedError()

        elif isinstance(node, xarray.Dataset):
            return EOGroup(node.name, attrs=node.attrs)  # type: ignore[arg-type]
        return EOVariable(node.name, node)  # type: ignore[arg-type]

    def __setitem__(self, key: str, value: "EOObject") -> None:
        from eopf.product.core.eo_variable import EOVariable

        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if not isinstance(value, EOVariable):
            raise NotImplementedError()
        self._ref[key] = value

    def __len__(self) -> int:
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return len(self._ref)

    def __iter__(self) -> Iterator[str]:
        return self.iter("")
