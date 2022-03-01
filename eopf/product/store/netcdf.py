import itertools as it
import os
import pathlib
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Optional, Union

from netCDF4 import Dataset, Group, Variable

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore

if TYPE_CHECKING:
    from eopf.product.core.eo_object import EOObject


class NetCDFStore(EOProductStore):
    RESTRICTED_ATTR_KEY = ("_FillValue",)

    def __init__(self, url: str) -> None:
        url = os.path.expanduser(url)
        super().__init__(url)
        self._root: Optional[Dataset] = None

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        self._root = Dataset(self.url, mode, **kwargs)

    def close(self) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        super().close()
        self._root.close()
        self._root = None

    def is_group(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(path)
        return isinstance(current_node, Group)

    def is_variable(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(path)
        return isinstance(current_node, Variable)

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(group_path)
        attrs = {attr: value for attr, value in attrs.items() if attr not in self.RESTRICTED_ATTR_KEY}
        if group_path not in ["", "/"]:
            current_node.setncatts(attrs)

    def iter(self, path: str) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(path)
        return it.chain(iter(current_node.groups), iter(current_node.variables))

    def __getitem__(self, key: str) -> "EOObject":
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")

        from eopf.product.core import EOGroup, EOVariable

        try:
            obj = self._select_node(key)
        except IndexError as e:  # if key is invalid, netcdf4 raise IndexError ...
            raise KeyError(e)
        if self.is_group(key):
            return EOGroup(attrs=obj.__dict__)
        print(obj)
        return EOVariable(data=obj, attrs=obj.__dict__, dims=obj.dimensions)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        from eopf.product.core import EOGroup, EOVariable

        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if isinstance(value, EOGroup):
            self._root.createGroup(key)
        elif isinstance(value, EOVariable):
            dimensions = []
            # FIXME: dimensions between value._data and value can mismatch ...
            for idx, (dim, _) in enumerate(zip(value.dims, value._data.dims)):
                if dim not in self._root.dimensions:
                    self._root.createDimension(dim, size=value._data.shape[idx])
                dimensions.append(dim)
            variable = self._root.createVariable(key, value._data.dtype, dimensions=dimensions)
            variable[:] = value._data.values
        else:
            raise TypeError("Only EOGroup and EOVariable can be set")
        self.write_attrs(key, value.attrs)

    def __len__(self) -> int:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return len(self._root.groups) + len(self._root.variables)

    def __iter__(self) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return it.chain(iter(self._root.groups), iter(self._root.variables))

    def _select_node(self, key: str) -> Union[Dataset, Group, Variable]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if key in ["/", ""]:
            return self._root
        return self._root[key]

    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".nc"]


class NetcdfStringToTime(EOProductStore):
    def __init__(self, url: str) -> None:
        url = os.path.expanduser(url)
        super().__init__(url)
        self._root = None

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        import xarray as xr

        self._root = xr.open_dataset(self.url)

    def close(self) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        super().close()
        self._root.close()
        self._root = None

    def is_group(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError

    def is_variable(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError

    def iter(self, path: str) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError

    def __getitem__(self, key: str) -> "EOObject":
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")

        import pandas as pd

        from eopf.product.core import EOVariable

        time_da = self._root.get(key)
        start = pd.to_datetime("1970-1-1T0:0:0.000000Z")
        end = pd.to_datetime(time_da)
        time_delta = (end - start) // pd.Timedelta("1microsecond")
        eov: EOVariable = EOVariable(data=time_delta, attrs={"unit": "microseconds since 1970-1-1T0:0:0.000000Z"})
        return eov

    def __setitem__(self, key: str, value: "EOObject") -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError

    def __len__(self) -> int:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return 1

    def __iter__(self) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError

    def _select_node(self, key: str) -> Union[Dataset, Group, Variable]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError
