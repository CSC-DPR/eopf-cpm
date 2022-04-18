import pathlib
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Optional, Union

import rioxarray
import xarray

from eopf.exceptions import StoreNotOpenError
from eopf.product.store.abstract import EOProductStore

if TYPE_CHECKING:  # pragma: no cover

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

    def __getitem__(self, key: str) -> "EOObject":
        from eopf.product.core.eo_group import EOGroup
        from eopf.product.core.eo_variable import EOVariable

        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        node = self._select_node(key)
        if isinstance(node, xarray.Variable):
            return EOVariable(data=node)

        if isinstance(node, xarray.core.coordinates.Coordinates):
            return EOGroup(
                variables={key: EOVariable(data=value.variable.to_base_variable()) for key, value in node.items()},
            )

        group = EOGroup()
        group["value"] = EOVariable(data=node.data)
        group["coordinates"] = EOGroup(
            variables={key: EOVariable(data=value.variable.to_base_variable()) for key, value in node.coords.items()},
        )
        return group

    def __iter__(self) -> Iterator[str]:
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return self.iter("")

    def __len__(self) -> int:
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return 2

    def __setitem__(self, key: str, value: "EOObject") -> None:  # pragma: no cover
        raise NotImplementedError

    # docstr-coverage: inherited
    def close(self) -> None:
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        super().close()
        self._mode = None
        self._ref = None

    # docstr-coverage: inherited
    @property
    def is_erasable(self) -> bool:
        return False

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        try:
            node = self._select_node(path)
            return isinstance(node, (xarray.core.coordinates.Coordinates, xarray.DataArray))
        except KeyError:
            return False

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        try:
            node = self._select_node(path)
            return isinstance(node, xarray.Variable)
        except KeyError:
            return False

    # docstr-coverage: inherited
    @property
    def is_writeable(self) -> bool:
        return False

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        node = self._select_node(path)
        if path in ["", "/"]:
            return iter(["value", "coordinates"])

        if isinstance(node, xarray.core.coordinates.Coordinates):
            return iter(node.keys())

        return iter([])

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open(mode=mode)
        if "chunks" not in kwargs:
            kwargs["chunks"] = True
        self._ref = rioxarray.open_rasterio(self.url, **kwargs)
        self._mode = mode

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:  # pragma: no cover
        raise NotImplementedError

    # docstr-coverage: inherited
    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".tiff", ".tif", ".jp2"]

    def _select_node(self, path: str) -> Union[xarray.DataArray, xarray.Variable, xarray.core.coordinates.Coordinates]:
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")

        if isinstance(self._ref, (list, xarray.Dataset)):
            raise NotImplementedError

        if path in ["", "/"]:
            return self._ref

        if path in ["value", "/value"]:
            return self._ref.variable

        if any(path.startswith(key) for key in ["coordinates", "/coordinates"]):
            path = path.partition("coordinates")[-1]
            if not path:
                return self._ref.coords
            if path.startswith("/"):
                path = path[1:]
            if path in self._ref.coords:
                return self._ref.coords[path].variable.to_base_variable()
        raise KeyError()
