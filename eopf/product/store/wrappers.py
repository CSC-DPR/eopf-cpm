import importlib
import numbers
from collections.abc import MutableMapping
from typing import Any, Iterator, Optional, Union

import xarray

from eopf.exceptions import StoreNotOpenError
from eopf.product.core import EOVariable
from eopf.product.core.eo_group import EOGroup
from eopf.product.core.eo_object import EOObject
from eopf.product.store.abstract import EOProductStore, StorageStatus


class FromAttributesToVariableAccessor(EOProductStore):

    store: Optional[EOProductStore]

    @property
    def status(self) -> StorageStatus:
        return self.store.status if self.store is not None else StorageStatus.CLOSE

    def open(
        self, mode: str = "r", store_cls: str = "", attr_name: str = "", index: Optional[Any] = None, **kwargs: Any
    ) -> None:
        module, _, klass = store_cls.rpartition(".")
        self.store = getattr(importlib.import_module(module), klass)(self.url)
        self.attr_name = attr_name
        self.index = index
        self.store.open(mode, **kwargs)  # type: ignore[union-attr]

    def close(self) -> None:
        if self.store is None:
            raise StoreNotOpenError()
        self.store.close()
        self.store = None

    def __getitem__(self, key: str) -> EOObject:
        if self.store is None:
            raise StoreNotOpenError()
        item = self.store[key]
        data = item.attrs[self.attr_name]
        if self.index is not None:
            data = data[self.index]
        if isinstance(data, (numbers.Number, str)):
            data = [data]
        return EOVariable(data=data)

    def __setitem__(self, key: str, value: EOObject) -> None:
        if self.store is None:
            raise StoreNotOpenError()

        if not isinstance(value, EOVariable):
            raise NotImplementedError()

        if key not in self.store:
            self.store[key] = EOGroup()
        base_: Union[list[xarray.DataArray], xarray.DataArray]
        if self.attr_name not in self.store[key].attrs:
            if self.index is not None:
                base_ = [value._data]
            else:
                base_ = value._data
        else:
            base_ = self.store[key].attrs[self.attr_name].append(value._data)

        self.store.write_attrs(key, {self.attr_name: base_})

    def __iter__(self) -> Iterator[str]:
        if self.store is None:
            raise StoreNotOpenError()
        return iter(self.store)

    def is_group(self, path: str) -> bool:
        return False

    def is_variable(self, path: str) -> bool:
        if self.store is None:
            raise StoreNotOpenError()
        return path in self.store

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = ...) -> None:
        raise NotImplementedError

    def iter(self, path: str) -> Iterator[str]:
        if self.store is None:
            raise StoreNotOpenError()
        return self.store.iter(path)

    def __len__(self) -> int:
        if self.store is None:
            raise StoreNotOpenError()
        return len(self.store)
