import pathlib
from typing import TYPE_CHECKING, Any, Iterator, MutableMapping, Optional

import zarr
from dask import array as da
from zarr.hierarchy import Group
from zarr.storage import FSStore, contains_array, contains_group

from eopf.exceptions import StoreNotOpenError
from eopf.product.utils import conv

from .abstract import EOProductStore

if TYPE_CHECKING:  # pragma: no cover
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

    # docstr-coverage: inherited
    def __init__(self, url: str) -> None:
        super().__init__(url)

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        self._mode = mode
        self._root: Group = zarr.open(store=self.url, mode=mode, **kwargs)
        self._fs = self._root.store

    # docstr-coverage: inherited
    def close(self) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before close it")

        # only if we write
        if any(self._mode.startswith(mode) for mode in ("w", "a")) or "+" in self._mode:
            zarr.consolidate_metadata(self._root.store)

        super().close()
        self._root = None
        self._fs = None

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return contains_group(self._fs, path=path)

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return contains_array(self._fs, path=path)

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self._root[group_path].attrs.update(conv(attrs))

    # docstr-coverage: inherited
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
            dask_array = da.asarray(value._data.data)  # .data is generally already a dask array.
            zarr_array = self._root.create(key, shape=dask_array.shape)
            # While it's possible to create the array with to_zarr,
            # it fail to create the array on some array shapes (ex : empty)
            da.to_zarr(dask_array, zarr_array)
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

    # docstr-coverage: inherited
    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".zarr", ".zip", ""]
