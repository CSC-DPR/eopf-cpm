import os
import pathlib
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Optional

import pygrib

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


class EONetCDFStore(EOProductStore):
    _DATA_KEY = "values"
    _COORDINATE_0_KEY = "distinctLatitudes"
    _COORDINATE_1_KEY = "distinctLongitudes"
    _RESTRICTED_ATTR_KEY = (
        "latLonValues",
        "latitudes",
        "longitudes",
        "Ni",
        "Nj",
        _DATA_KEY,
        _COORDINATE_0_KEY,
        _COORDINATE_1_KEY,
    )

    def __init__(self, url: str) -> None:
        url = os.path.expanduser(url)
        super().__init__(url)
        # open is the file reader class.
        self._root: Optional[pygrib.open] = None
        # gribmessage is a lazilly read message class.
        self._message_dict: dict[str, pygrib.gribmessage] = dict()

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        if mode != "r":
            raise NotImplementedError
        super().open()
        # open is a class (constructor).
        self._root = pygrib.open(self.url)  # no mode/additional option.
        for message in self._root:
            self._message_dict[message.short_name] = message

    def close(self) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        super().close()
        self._root.close()
        self._root = None

    def is_group(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return path in ["", "/"]

    def is_variable(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return path in self._message_dict

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError

    def iter(self, path: str) -> Iterator[str]:
        if path not in ["", "/"]:
            raise KeyError()
        return iter(self)

    def __getitem__(self, key: str) -> "EOObject":
        if not self.is_variable(key):
            raise KeyError()
        message = self._message_dict[key]

        from eopf.product.core import EOVariable

        # Copy all non restricted attributes (restricted include multiple representations of data and coordinates)
        attributes = {attr: getattr(message, attr) for attr in message.keys() if attr not in self._RESTRICTED_ATTR_KEY}
        dimensions = (message.Ni, message.Nj)
        # TODO : manage coordinates. Maybe put data into measurements, and coordinates into corresponding coordinates.
        return EOVariable(data=getattr(message, self._DATA_KEY), attrs=attributes, dims=dimensions)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        raise NotImplementedError

    def __len__(self) -> int:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return len(self._message_dict)

    def __iter__(self) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter(self._message_dict)

    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".grib"]
