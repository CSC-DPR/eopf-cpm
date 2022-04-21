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
    """Accessor that wrap a data format accessor to extract
    attribute and map it to an EOVariable

    Parameters
    ----------
    url: str
        url path to open the data format accessor

    Attributes
    ----------
    store: EOProductStore
        accessor to extract base element
    attr_name: str
        attribute name to extract
    index: any, optional
        index to extract on the attribute sequence
    """

    store: Optional[EOProductStore] = None

    # docstr-coverage: inherited
    @property
    def status(self) -> StorageStatus:
        return self.store.status if self.store is not None else StorageStatus.CLOSE

    def open(
        self, mode: str = "r", store_cls: str = "", attr_name: str = "", index: Optional[Any] = None, **kwargs: Any
    ) -> None:
        """Open the store in the given mode

        Parameters
        ----------
        mode: str, optional
            mode to open the store
        store_cls: str
            full python path to the store class (ex: eopf.product.store.EOZarrStore)
        attr_name: str
            name of the attribute to convert
        index: any, optional
            index to extract on the attribute sequence
        **kwargs: Any
            extra kwargs of open for wrapped store
        """
        module, _, klass = store_cls.rpartition(".")
        self.store = getattr(importlib.import_module(module), klass)(self.url)
        self.attr_name = attr_name
        self.index = index
        self.store.open(mode, **kwargs)  # type: ignore[union-attr]

    # docstr-coverage: inherited
    def close(self) -> None:
        if self.store is None:
            raise StoreNotOpenError()
        self.store.close()
        self.store = None

    def __getitem__(self, key: str) -> EOObject:
        data = self._extract_data(key)
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

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        # to have an harmonized behavior with is_variable
        if self.store is None:
            raise StoreNotOpenError()
        return False

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        if self.store is None:
            raise StoreNotOpenError()
        return path in self.store

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = ...) -> None:
        raise NotImplementedError

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        if self.store is None:
            raise StoreNotOpenError()
        return self.store.iter(path)

    def __len__(self) -> int:
        if self.store is None:
            raise StoreNotOpenError()
        return len(self.store)

    def _extract_data(self, key: str) -> list[Any]:
        """Retrieve data from the attribute of the element at the given key.

        Parameters
        ----------
        key: str
            element path where the attribute will be extracted

        Returns
        -------
        list of data
        """
        if self.store is None:
            raise StoreNotOpenError()
        item = self.store[key]
        data = item.attrs[self.attr_name]
        if self.index is not None:
            data = data[self.index]
        if isinstance(data, (numbers.Number, str)):
            data = [data]
        return data


class FromAttributesToFlagValueAccessor(FromAttributesToVariableAccessor):
    """Accessor that wrap a data format accessor to extract
    attribute and map it to an EOVariable from corresponding flag value

    Parameters
    ----------
    url: str
        url path to open the data format accessor

    Attributes
    ----------
    store: EOProductStore
        accessor to extract base element
    attr_name: str
        attribute name to extract
    index: any, optional
        index to extract on the attribute sequence
    flag_meanings: str
        space separeted list of flag name use to convert data
    flag_values: list[int]
        list of value corresponding to flag meaning
    """

    def open(
        self,
        mode: str = "r",
        store_cls: str = "",
        attr_name: str = "",
        index: Optional[Any] = None,
        flag_meanings: str = "",
        flag_values: list[int] = [],
        **kwargs: Any,
    ) -> None:
        """Open the store in the given mode

        Parameters
        ----------
        mode: str, optional
            mode to open the store
        store_cls: str
            full python path to the store class (ex: eopf.product.store.EOZarrStore)
        attr_name: str
            name of the attribute to convert
        index: any, optional
            index to extract on the attribute sequence
        flag_meanings: str
            space separeted list of flag name use to convert data
        flag_values: list[int]
            list of value corresponding to flag meaning
        **kwargs: Any
            extra kwargs of open for wrapped store
        """
        module, _, klass = store_cls.rpartition(".")
        self.store = getattr(importlib.import_module(module), klass)(self.url)
        self.attr_name = attr_name
        self.index = index
        self.store.open(mode, **kwargs)  # type: ignore[union-attr]
        self.flag_meanings = flag_meanings.split(" ")
        self.flag_values = flag_values

    # docstr-coverage: inherited
    def __getitem__(self, key: str) -> EOObject:
        data = self._apply_flags(self._extract_data(key))
        return EOVariable(data=data)

    def _apply_flags(self, data: list[Any]) -> list[int]:
        """Map the given data values to flag values

        Parameters
        ----------
        data: list
            list of data to map

        Returns
        -------
        mapped list
        """
        return [self.flag_values[self.flag_meanings.index(str(d))] for d in data]
