import pathlib
from typing import TYPE_CHECKING, Any, Iterator, MutableMapping, Optional

import zarr
from zarr.hierarchy import Group
from zarr.storage import FSStore, contains_array, contains_group

from eopf.exceptions import StoreNotOpenError

from .abstract import EOProductStore

if TYPE_CHECKING:
    from eopf.product.core.eo_object import EOObject


class EOZarrStore(EOProductStore):
    """Store representation to access to a Zarr file on the given URL

    Parameters
    ----------
    url: str
        path url or the target store

    Attributes
    ----------
    url: str
        url to the target store
    sep: str
        file separator

    See Also
    -------
    zarr.storage
    """

    _root: Optional[Group] = None
    _fs: Optional[FSStore] = None
    sep = "/"

    def __init__(self, url: str) -> None:
        super().__init__(url)

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        self._root: Group = zarr.open(store=self.url, mode=mode, **kwargs)
        self._fs = self._root.store

    def close(self) -> None:
        super().close()
        self._root = None
        self._fs = None

    def is_group(self, path: str) -> bool:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return contains_group(self._fs, path=path)

    def is_variable(self, path: str) -> bool:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return contains_array(self._fs, path=path)

    def conv(self, obj: Any) -> Any:
        # check if list or tuple
        if isinstance(obj, list) or isinstance(obj, tuple):
            tmp_lst = []
            for element in obj:
                tmp_lst.append(self.conv(element))
            if isinstance(obj, list):
                return tmp_lst
            return tuple(tmp_lst)

        # check int
        try:
            int(obj)
        except (ValueError, TypeError):
            pass
        else:
            return int(obj)

        # check float
        try:
            float(obj)
        except (ValueError, TypeError):
            pass
        else:
            return float(obj)

        # check str
        try:
            str(obj)
        except (ValueError, TypeError):
            pass
        else:
            return str(obj)

        # if no conversion can be done
        raise Exception(f"Can NOT convert {obj} of type {type(obj)}")

    def attrs_convert(self, d: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        for key, value in d.items():
            if isinstance(d[key], MutableMapping):
                d[key] = self.attrs_convert(value)
            else:
                d[key] = self.conv(value)
        return d

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self._root[group_path].attrs.update(self.attrs_convert(attrs))

    def iter(self, path: str) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter(self._root.get(path, []))

    def __getitem__(self, key: str) -> "EOObject":
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")

        from eopf.product.core import EOGroup, EOVariable

        obj = self._root[key]
        if self.is_group(key):
            return EOGroup(attrs=obj.attrs)
        return EOVariable(data=obj, attrs=obj.attrs)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        from eopf.product.core import EOGroup, EOVariable

        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if isinstance(value, EOGroup):
            self._root.create_group(key, overwrite=True)
        elif isinstance(value, EOVariable):
            self._root.create_dataset(key, data=value._data.values)
            if hasattr(self._root.store, "path"):
                zarr.consolidate_metadata(self.sep.join([self._root.store.path, self._root[key].path]))
        else:
            raise TypeError("Only EOGroup and EOVariable can be set")
        self.write_attrs(key, value.attrs)

    def __delitem__(self, key: str) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        del self._root[key]

    def __len__(self) -> int:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return len(self._root)

    def __iter__(self) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter(self._root)

    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".zarr", ".zip", ""]
