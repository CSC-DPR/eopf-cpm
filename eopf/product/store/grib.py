import os
import pathlib
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Optional

import xarray as xr

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore
from eopf.product.utils import downsplit_eo_path

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


class EOGribAccessor(EOProductStore):
    _DATA_KEY = "values"
    _COORDINATE_0_KEY = "distinctLatitudes"
    _COORDINATE_1_KEY = "distinctLongitudes"

    def __init__(self, url: str) -> None:
        url = os.path.expanduser(url)
        super().__init__(url)
        # open is the file reader class.
        self._ds: Optional[xr.Dataset] = None

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        if mode != "r":
            raise NotImplementedError
        super().open()
        # open is a class (constructor).
        self._ds = xr.open_dataset(self.url, engine="cfgrib")

    def close(self) -> None:
        if self._ds is None:
            raise StoreNotOpenError("Store must be open before access to it")
        super().close()
        self._ds.close()
        self._ds = None

    def is_group(self, path: str) -> bool:
        if self._ds is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return path in ["", "/", "coordinates", "/coordinates", "coordinates/", "/coordinates/"]

    def is_variable(self, path: str) -> bool:
        if self._ds is None:
            raise StoreNotOpenError("Store must be open before access to it")
        group, sub_path = downsplit_eo_path(path)
        if group == "coordinates":
            return sub_path in self._ds.coords
        return path in self._ds

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError

    def iter(self, path: str) -> Iterator[str]:
        if self._ds is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if path in ["", "/"]:
            yield "coordinates"
            yield from self._ds.keys()
            return
        if path in ["coordinates", "/coordinates", "coordinates/", "/coordinates/"]:
            yield from self._ds.coords.keys()
            return
        raise KeyError()

    def __getitem__(self, key: str) -> "EOObject":
        from eopf.product.core import EOGroup, EOVariable

        if self._ds is None:
            raise StoreNotOpenError("Store must be open before access to it")

        if self.is_group(key):
            return EOGroup()
        if not self.is_variable(key):
            raise KeyError()
        group, sub_path = downsplit_eo_path(key)
        if group == "coordinates":
            data = self._ds.coords[sub_path]
        else:
            data = self._ds[key]
        return EOVariable(data=data)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        raise NotImplementedError

    def __len__(self) -> int:
        if self._ds is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return 1 + len(self._ds)

    def __iter__(self) -> Iterator[str]:
        return self.iter("")

    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".grib"]
