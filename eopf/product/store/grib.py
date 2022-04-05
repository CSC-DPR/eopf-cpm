import os
import pathlib
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Optional

import xarray as xr

from eopf.exceptions import StoreNotOpenError
from eopf.product.store.abstract import EOReadOnlyStore
from eopf.product.utils import downsplit_eo_path

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


class EOGribAccessor(EOReadOnlyStore):
    _DATA_KEY = "values"
    _COORDINATE_0_KEY = "distinctLatitudes"
    _COORDINATE_1_KEY = "distinctLongitudes"

    def __init__(self, url: str) -> None:
        url = os.path.expanduser(url)
        super().__init__(url)
        # open is the file reader class.
        self._ds: Optional[xr.Dataset] = None

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open(mode, **kwargs)
        # open is a class (constructor).
        self._ds = xr.open_dataset(self.url, engine="cfgrib", **kwargs)

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

    def iter(self, path: str) -> Iterator[str]:
        if self._ds is None:
            raise StoreNotOpenError("Store must be open before access to it")
        keys: Iterable[str]
        if path in ["", "/"]:
            keys = ["coordinates", *self._ds.keys()]
        elif path in ["coordinates", "/coordinates", "coordinates/", "/coordinates/"]:
            keys = self._ds.coords.keys()
        else:
            raise KeyError(f"key {path} not exist")
        return iter(keys)

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

    def __len__(self) -> int:
        if self._ds is None:
            raise StoreNotOpenError("Store must be open before access to it")
        # We have one group (coordinates).
        return 1 + len(self._ds)

    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".grib"]
