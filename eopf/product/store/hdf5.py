from collections.abc import MutableMapping
import itertools as it
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Optional, Dict, Union

import h5py
import xarray as xr

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore
from eopf.exceptions import StoreNotOpenError
from eopf.product.utils import join_path, weak_cache
from ..core.eo_variable import EOVariable
from ..core.eo_group import EOGroup

if TYPE_CHECKING:
    from eopf.product.core.eo_object import EOObject


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
        #return it.chain(iter(current_node.keys()), iter(current_node.variables))
        return it.chain(iter(current_node.keys()))

    def __getitem__(self, key: str) -> "EOObject":
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")

        obj = self._select_node(key)
        if self.is_group(key):
            return EOGroup(attrs=obj.attrs)
        return EOVariable(data=obj, attrs=obj.attrs)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        if self._fs is None:
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
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return len(self._root)
    
    def _select_node(self, key: str) -> Union[h5py.Group, h5py.Dataset]:
        if key in ["/", ""]:
            return self._root
        return self._root[key]
