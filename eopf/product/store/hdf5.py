from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Optional

import h5py

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore

if TYPE_CHECKING:
    from eopf.product.core.eo_object import EOObject


class EOHDF5Store(EOProductStore):

    _fs: Optional[h5py.File] = None

    def __init__(self, url: str) -> None:
        super().__init__(url)

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open(mode=mode, **kwargs)
        self._fs = h5py.File(self.url, mode=mode)

    def close(self) -> None:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before close")
        self._fs.close()
        super().close()

    def is_group(self, path: str) -> bool:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return isinstance(self._fs[path], h5py.Group)

    def is_variable(self, path: str) -> bool:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return isinstance(self._fs[path], h5py.Dataset)

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if group_path == "":
            group_path = "/"
        self._fs[group_path].attrs.update(attrs)

    def iter(self, path: str) -> Iterator[str]:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter(self._fs[path])

    def __getitem__(self, key: str) -> "EOObject":
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        from eopf.product.core import EOGroup, EOVariable

        obj = self._fs[key]
        if self.is_group(key):
            return EOGroup(attrs=obj.attrs)
        return EOVariable(data=obj[()], attrs=obj.attrs, chunks=obj.chunks)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        from eopf.product.core import EOGroup, EOVariable

        if isinstance(value, EOGroup):
            self._fs.create_group(key)
        elif isinstance(value, EOVariable):
            self._fs.create_dataset(key, value._data)
        else:
            raise TypeError("Only EOGroup and EOVariable can be set")
        self.write_attrs(key, value.attrs)

    def __iter__(self) -> Iterator[str]:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter(self._fs)

    def __len__(self) -> int:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return len(self._fs)
