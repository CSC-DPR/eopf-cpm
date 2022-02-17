from typing import Any, Iterable, Iterator, MutableMapping, Optional

import xarray
import zarr
from zarr.hierarchy import Group
from zarr.storage import FSStore, array_meta_key, contains_array, contains_group

from eopf.exceptions import StoreNotOpenError
from eopf.product.utils import join_path, weak_cache

from .abstract import EOProductStore


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

    def get_data(self, key: str) -> tuple[Optional[xarray.Dataset], dict[str, Any]]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if self.is_group(key):
            dataset = None
            group = self[key]
            if self.has_variables(group.path):
                dataset = self.__get_dataset(key)
            return (dataset, group.attrs.asdict())
        elif self.is_variable(key):
            raise TypeError("EOVariable must be loaded from EOGroup dataset, not EOProductStore")
        raise KeyError(f"Invalid name {key}")

    def add_group(self, name: str, relative_path: Iterable[str] = [], attrs: MutableMapping[str, Any] = {}) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self._root.create_group(join_path(*relative_path, name, sep=self.sep)).attrs.update(attrs)

    def update_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self._root[group_path].attrs.update(attrs)

    def delete_attr(self, group_path: str, attr_name: str) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        del self._root[group_path].attrs[attr_name]

    def add_variables(self, name: str, dataset: xarray.Dataset, relative_path: Iterable[str] = []) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        dataset.to_zarr(store=join_path(self.url, *relative_path, name, sep=self.sep), mode="a")

    def iter(self, path: str) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter(self._root.get(path, []))

    def has_variables(self, path: str) -> bool:
        """check if a path contains eovariables

        Parameters
        ----------
        path: str
            container to check

        Returns
        -------
        bool
            the path contain variables representation or not
        """
        return any(
            array_meta_key in self._fs.listdir(path=join_path(path, key))
            for key in self._fs.listdir(path=path)
            if not key.startswith(".zarr")
        )

    @weak_cache
    def __get_dataset(self, path: str) -> Any:
        return xarray.open_zarr(join_path(self.url, path, sep=self.sep), consolidated=True)

    def __getitem__(self, key: str) -> Any:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return self._root[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        raise NotImplementedError()

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
