from typing import Any, Hashable, Iterable, Iterator, MutableMapping, Optional

import fsspec
import xarray
import zarr
from zarr.hierarchy import Group
from zarr.storage import FSStore, array_meta_key, contains_array, contains_group

from eopf.exceptions import StoreNotOpenError
from eopf.product.utils import join_path, weak_cache

from .abstract import EOProductStore


class EOZarrStore(EOProductStore):
    """Store representation to access to a Zarr file on the given URL

    Attributes
    ----------
    status
    is_readable
    is_writeable
    is_listable
    is_erasable
    url: str
        url to the target store
    sep: str
        file separator
    """

    _root: Optional[Group] = None
    _fs: Optional[FSStore] = None
    sep = "/"

    def __init__(self, url: str) -> None:
        super().__init__(url)

    @property
    def map(self) -> Optional[fsspec.FSMap]:
        """FSMap accessor"""
        return self._fs.map if self._fs is not None and hasattr(self._fs, "map") else None

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        """open the store in the given mode

        Parameters
        ----------
        mode: str, optional
            mode to open the store
        **kwargs: Any
            extra kwargs of open on zarr librairy
        See Also
        --------
        zarr.open
        """

        super().open()
        self._root: Group = zarr.open(store=self.url, mode=mode, **kwargs)
        self._fs = self._root.store

    def close(self) -> None:
        """close the store"""
        super().close()
        self._root = None
        self._fs = None

    def listdir(self, path: Optional[str] = None) -> Any:
        """list the given path on the store, or the root if no path.

        Parameters
        ----------
        path: str, optional
            path to list on the store
        """
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return self._fs.listdir(path=path)

    def rmdir(self, path: Optional[str] = None) -> None:
        """remove the given path on the store, or the root if no path.

        Parameters
        ----------
        path: str, optional
            path to remove on the store
        """
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self._fs.rmdir(path=path)

    def clear(self) -> None:
        """clear all the store from root path"""
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self._fs.clear()

    def getsize(self, path: Optional[str] = None) -> Any:
        """return size under the path or root if no path given

        Parameters
        ----------
        path: str, optional
            path to get size on the store
        """
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return self._fs.getsize(path=path)

    def dir_path(self, path: Optional[str] = None) -> Any:
        """return directory path of the given path or root

        Parameters
        ----------
        path: str, optional
            path to get directory on the store
        """
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return self._fs.dir_path(path=path)

    def is_group(self, path: str) -> bool:
        """check if the given path under root corresponding to a group representation

        Parameters
        ----------
        path: str
            path to check
        """
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return contains_group(self._fs, path=path)

    def is_variable(self, path: str) -> bool:
        """check if the given path under root corresponding to a variable representation

        Parameters
        ----------
        path: str
            path to check
        """
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return contains_array(self._fs, path=path)

    def get_data(self, key: str) -> tuple[Any, ...]:
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

    def __getitem__(self, key: str) -> Any:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return self._root[key]

    @weak_cache
    def __get_dataset(self, path: str) -> Any:
        """open the given path as dataset with xarray

        Parameters
        ----------
        path: str
            path to open as xarray.Dataset
        See Also
        --------
        xarray.open_zarr
        xarray.Dataset
        """
        return xarray.open_zarr(join_path(self.url, path, sep=self.sep), consolidated=True)

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

    def add_group(
        self,
        name: str,
        relative_path: Iterable[str] = [],
        attrs: MutableMapping[Hashable, Any] = {},
    ) -> None:
        """write a group over the store

        Parameters
        ----------
        name: str
            name of the group
        relative_path: Iterable[str], optional
            list of all parents from root
        """
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
        """write variables over the store

        Parameters
        ----------
        name: str
            name of the dataset
        relative_path: Iterable[str], optional
            list of all parents from root
        """
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        dataset.to_zarr(store=join_path(self.url, *relative_path, name, sep=self.sep), mode="a")

    def has_variables(self, path: str) -> bool:
        """check if a path contains eovariables

        Parameters
        ----------
        path: str
            container to check
        """
        return any(
            array_meta_key in self.listdir(join_path(path, key))
            for key in self.listdir(path)
            if not key.startswith(".zarr")
        )

    def iter(self, path: str) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter(self._root.get(path, []))
