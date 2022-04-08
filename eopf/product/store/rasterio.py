import pathlib
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Optional, Union

import rioxarray
import xarray

from eopf.exceptions import StoreNotOpenError
from eopf.product.store.abstract import EOProductStore

if TYPE_CHECKING:  # pragma: no cover
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

    def __getitem__(self, key: str) -> "EOObject":
        from eopf.product.core.eo_group import EOGroup
        from eopf.product.core.eo_variable import EOVariable

        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        node = self._select_node(key)
        group = EOGroup()
        group["data"] = EOVariable(data=node.data)
        group["coordinates"] = EOGroup(
            variables={key: EOVariable(data=value.variable.to_base_variable()) for key, value in node.coords.items()},
        )
        return group

    def __iter__(self) -> Iterator[str]:
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter([""])

    def __len__(self) -> int:
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return 1

    def __setitem__(self, key: str, value: "EOObject") -> None:
        from eopf.product.core.eo_variable import EOVariable

        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if not isinstance(value, EOVariable):
            raise NotImplementedError()
        self._ref[key] = value

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
    @property
    def is_erasable(self) -> bool:
        return False

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        return not self.is_variable(path)

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        node = self._select_node(path)
        return isinstance(node, (xarray.DataArray, xarray.Variable))

    # docstr-coverage: inherited
    @property
    def is_writeable(self) -> bool:
        return False

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        self._select_node(path)
        return iter(["data", "coordinates"])

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open(mode=mode)
        self._ref = rioxarray.open_rasterio(self.url, **kwargs)
        self._mode = mode
        self._lock = kwargs.get("lock")

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if not self.is_variable(group_path):
            raise NotImplementedError()
        node = self._select_node(group_path)
        node.attrs.update(attrs)  # type: ignore[arg-type]

    # docstr-coverage: inherited
    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".tiff", ".tif", ".jp2"]

    def _select_node(self, path: str) -> Union[xarray.DataArray, xarray.Variable, dict[str, xarray.Variable]]:
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")

        if isinstance(self._ref, (list, xarray.Dataset)):
            raise NotImplementedError

        if path in ["", "/"]:
            return self._ref
        raise KeyError()
