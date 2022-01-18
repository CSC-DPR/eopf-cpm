from functools import cache
from typing import Any, Iterator, Optional

import fsspec
import xarray
import zarr
from zarr.hierarchy import Group
from zarr.storage import FSStore, array_meta_key, contains_array, contains_group

from eopf.product.utils import join_path

from .abstract import EOProductStore


class EOZarrStore(EOProductStore):

    _root: Optional[Group] = None
    _fs: Optional[FSStore] = None
    sep = "/"

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.__get_dataset = cache(self.__get_dataset)

    @property
    def map(self) -> fsspec.FSMap:
        return self._fs.map

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        self._root: Group = zarr.open(store=self.url, mode=mode, **kwargs)
        self._fs = self._root.store
        return self

    def close(self) -> None:
        super().close()
        self._root = None
        self._fs = None

    def listdir(self, path: Optional[str] = None) -> list:
        return self._fs.listdir(path=path)

    def rmdir(self, path: Optional[str] = None) -> None:
        self._fs.rmdir(path=path)

    def clear(self) -> None:
        self._fs.clear()

    def getsize(self, path: Optional[str] = None) -> None:
        return self._fs.getsize(path=path)

    def dir_path(self, path: Optional[str] = None) -> None:
        return self._fs.dir_path(path=path)

    def is_group(self, path) -> bool:
        return contains_group(self._fs, path=path)

    def is_variable(self, path) -> bool:
        return contains_array(self._fs, path=path)

    def __getitem__(self, key: str) -> tuple:
        *relative_path, name = key.split("/")
        if self.is_group(key):
            dataset = None
            group = self._root[key]
            if self.has_variables(group):
                dataset = self.__get_dataset(key)
            return (name, relative_path, dataset, group.attrs.asdict())
        elif self.is_variable(key):
            raise TypeError("EOVariable must be loaded from EOGroup dataset, not EOProductStore")
        raise KeyError(f"Invalid name {key}")

    def __get_dataset(self, path) -> xarray.Dataset:
        return xarray.open_zarr(join_path(self.url, path, sep=self.sep), consolidated=True)

    def __setitem__(self, key: Any, value: Any):
        raise NotImplementedError()

    def __delitem__(self, key: str) -> None:
        del self._root[key]

    def __len__(self) -> int:
        return len(self._root)

    def __iter__(self) -> Iterator[str]:
        return iter(self._root)

    def add_group(self, name: str, relative_path: list[str] = []) -> None:
        self._root.create_group(join_path(*relative_path, name, sep=self.sep))

    def add_variables(self, name: str, dataset: xarray.Dataset, relative_path: list[str] = []) -> None:
        dataset.to_zarr(store=join_path(self.url, *relative_path, name, sep=self.sep), mode="a")

    def has_variables(self, container) -> bool:
        return any(
            array_meta_key in self.listdir(join_path(container.path, key))
            for key in self.listdir(container.path)
            if not key.startswith(".zarr")
        )
