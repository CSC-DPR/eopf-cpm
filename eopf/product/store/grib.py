import os
import pathlib
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Optional

import pygrib

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore
from eopf.product.utils import downsplit_eo_path

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


class EOGribStore(EOProductStore):
    _DATA_KEY = "values"
    _COORDINATE_0_KEY = "distinctLatitudes"
    _COORDINATE_1_KEY = "distinctLongitudes"
    _DIM_ATTR_MAPPING = {
        _DATA_KEY: ("Ni", "Nj"),
        _COORDINATE_0_KEY: ("Ni",),
        _COORDINATE_1_KEY: ("Nj",),
    }
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
        return path in ["", "/", "coordinates", "/coordinates", "coordinates/", "/coordinates/"]

    def is_variable(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        dict_path, _ = self._dict_path(path)
        return dict_path in self._message_dict

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        raise NotImplementedError

    def iter(self, path: str) -> Iterator[str]:
        if path in ["", "/"]:
            yield "coordinates"
            yield from self._message_dict
            return
        if path == "coordinates":
            for message_key in self._message_dict:
                yield f"{message_key}_lon"
                yield f"{message_key}_lat"
            return
        raise KeyError()

    def __getitem__(self, key: str) -> "EOObject":
        from eopf.product.core import EOGroup, EOVariable

        if self.is_group(key):
            return EOGroup()
        if not self.is_variable(key):
            raise KeyError()
        path, data_attr_key = self._dict_path(key)
        message = self._message_dict[path]

        # Copy all non restricted attributes (restricted include multiple representations of data and coordinates)
        attributes = {attr: getattr(message, attr) for attr in message.keys() if attr not in self._RESTRICTED_ATTR_KEY}
        dimensions = self._construct_dims(message, data_attr_key)
        return EOVariable(data=getattr(message, data_attr_key), attrs=attributes, dims=dimensions)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        raise NotImplementedError

    def __len__(self) -> int:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return len(self._message_dict)

    def __iter__(self) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return self.iter("")

    def _dict_path(self, path: str) -> tuple[str, str]:
        """Return corresponding message path, and data key (data, coor1 or coord2)"""
        group, sub_path = downsplit_eo_path(path)
        if group == "coordinates":
            if not sub_path:
                raise ValueError("Can't use _dict_path on the path of a group.")
            if path.endswith("_lon"):
                return sub_path[:-4], self._COORDINATE_1_KEY
            if path.endswith("_lat"):
                return sub_path[:-4], self._COORDINATE_0_KEY
            raise KeyError()
        return path, self._DATA_KEY

    def _construct_dims(self, message: pygrib.gribmessage, attr_key: str) -> tuple[str, ...]:
        """Get the dimensions names of the array in attr_key of message."""
        # Attributes containing the dimensions.
        dim_attrs = self._DIM_ATTR_MAPPING[attr_key]
        return tuple(getattr(message, dim_attr) for dim_attr in dim_attrs)

    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".grib"]
