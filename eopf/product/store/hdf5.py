import itertools as it
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Union

import h5py
import xarray as xr

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore

from ..core.eo_group import EOGroup
from ..core.eo_variable import EOVariable

if TYPE_CHECKING:
    from eopf.product.core.eo_object import EOObject


class H5ls:
    """This class displays the structure of a hdf5 file, i.e. the names of groups and variables"""

    def __init__(self):
        self.names = []

    def __call__(self, name: str):
        if name not in self.names:
            self.names += [name]

    def get_items(self):
        return len(self.names)


class EOHDF5Store(EOProductStore):
    def __init__(self, url: str) -> None:
        super().__init__(url)

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        self._root = h5py.File(self.url, mode=mode)

    def close(self) -> None:
        super().close()
        self._root.close()
        self._root = None

    def is_group(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(path)
        return isinstance(current_node, h5py.Group)

    def is_variable(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(path)
        return isinstance(current_node, h5py.Dataset)

    def write_attrs(self, path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(path)
        current_node.attrs.update(attrs)

    def iter(self, path: str) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(path)
        # return it.chain(iter(current_node.keys()), iter(current_node.variables))
        return it.chain(iter(current_node.keys()))

    def __getitem__(self, key: str) -> "EOObject":
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")

        obj = self._select_node(key)
        if self.is_group(key):
            return EOGroup(name=key, attrs=obj.attrs)
        return EOVariable(name=key, data=obj, attrs=obj.attrs)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")

        if isinstance(value, EOGroup):
            self._root.create_group(key)
        elif isinstance(value, EOVariable):
            da = xr.DataArray(data=value._data)
            self._root.create_dataset(key, data=da)
        else:
            raise TypeError("Only EOGroup and EOVariable can be set")
        self.write_attrs(key, value.attrs)

    def __iter__(self) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return it.chain(iter(self._root.keys()))

    def __len__(self) -> int:
        h5ls = H5ls()
        self._root.visititems(h5ls)
        return h5ls.get_items()

    def _select_node(self, key: str) -> Union[h5py.Group, h5py.Dataset]:
        if key in ["/", ""]:
            return self._root
        return self._root[key]
