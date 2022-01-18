import weakref
from collections.abc import MutableMapping
from typing import Any, Iterable, Iterator, Mapping, Optional, Union

import xarray

from eopf.exceptions import InvalidProductError, StoreNotDefinedError
from eopf.product.utils import join_path

from .formatting import renderer
from .mixins import EOVariableOperatorsMixin
from .store.abstract import EOProductStore, StorageStatus


class EOVariable(EOVariableOperatorsMixin):
    def __init__(self, name: str, data: Any, product: "EOProduct", **kwargs: Any):
        self._data: xarray.DataArray
        self._name: str = name
        self._product: EOProduct = weakref.proxy(product) if not isinstance(product, weakref.ProxyType) else product
        if isinstance(data, xarray.DataArray):
            self._data = data
        else:
            self._data = xarray.DataArray(data=data, name=name)

    @property
    def name(self) -> str:
        return self._name

    @property
    def attrs(self) -> Mapping:
        return self._data.attrs

    @property
    def dims(self) -> tuple:
        return self._data.dims

    def __getitem__(self, key: Any) -> "EOVariable":
        return EOVariable(key, self._data[key], self._product)

    def __setitem__(self, key: Any, value: Any) -> None:
        self._data[key] = value

    def __delitem__(self, key: Any) -> None:
        del self._data[key]

    def __iter__(self) -> Iterator["EOVariable"]:
        for data in self._data:
            yield EOVariable(data.name, data, self._product)

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOVariable]{hex(id(self))}"

    def _repr_html_(self):
        return renderer("variable.html", variable=self)


class EOGroup(MutableMapping[str, Union[EOVariable, "EOGroup"]]):
    """"""

    def __init__(
        self,
        name: str,
        product: "EOProduct",
        relative_path: Optional[list[str]] = None,
        dataset: Optional[xarray.Dataset] = None,
        attrs: Optional[dict[str, Any]] = None,
    ) -> None:
        self._name = name

        if relative_path is None:
            relative_path = []

        self._relative_path = relative_path
        self._dataset: Optional[xarray.Dataset] = dataset
        self._product: EOProduct = weakref.proxy(product) if not isinstance(product, weakref.ProxyType) else product
        self._items: dict[str, Optional[Union[EOVariable, "EOGroup"]]] = {}
        self._attrs = attrs or dict()

    def __getitem__(self, key: str) -> Union[EOVariable, "EOGroup"]:
        return self._get_item(key)

    def _get_item(self, key: str) -> Union[EOVariable, "EOGroup"]:
        if self._dataset is not None and key in self._dataset:
            return EOVariable(key, self._dataset[key], self._product)
        item = self._items.get(key)
        if item is None and self._store is None:
            raise KeyError(f"Invalide EOGroup item name {key}")
        elif item is None:
            name, relative_path, dataset, attrs = self._store[self._relative_key(key)]
            item = EOGroup(name, self._product, relative_path=relative_path, dataset=dataset, attrs=attrs)
        self[key] = item
        return item

    def __setitem__(self, key: str, value: Union[EOVariable, "EOGroup"]) -> None:
        if isinstance(value, EOGroup):
            self._items[key] = value
        elif isinstance(value, EOVariable):
            if self._dataset is None:
                self._dataset = xarray.Dataset()
            self._dataset[value.name] = value
        else:
            raise TypeError(f"Item assigment Impossible for type {type(value)}")

    def __delitem__(self, key: str) -> None:
        if key in self._items:
            del self._items[key]
        if self._store is not None and (store_key := self._relative_key(key)) in self._store:
            del self._store[store_key]

    def __iter__(self) -> Iterator[str]:
        for key in self._store[self._path]:
            if key not in self._items:
                yield key
        yield from self._items

    def __len__(self) -> int:
        length = len(self.items)
        if self._store is not None:
            length += len(key for key in self._store[self._path] if key not in self._items)
        return length

    def __getattr__(self, attr: str):
        return self[attr]

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOGroup]{hex(id(self))}"

    def _repr_html_(self):
        return renderer("group.html", group=self)

    def to_product(self) -> "EOProduct":
        ...

    @property
    def _store(self) -> Optional[EOProductStore]:
        return self._product._store

    @property
    def _path(self) -> str:
        if self._store is None:
            raise StoreNotDefinedError("Store must be defined")
        return join_path(*self._relative_path, self._name, sep=self._store.sep)

    @property
    def name(self) -> str:
        return self._name

    @property
    def attrs(self) -> Mapping:
        return self._attrs

    @property
    def dims(self) -> tuple:
        return self._dataset.dims if self._dataset else tuple()

    @property
    def groups(self) -> Iterable[tuple[str, "EOGroup"]]:
        for key, value in self._items.items():
            if isinstance(value, EOGroup):
                yield key, value

    @property
    def values(self) -> Iterable[tuple[str, EOVariable]]:
        for key, value in self._items.items():
            if isinstance(value, EOVariable):
                yield key, value

    def _relative_key(self, key: str) -> str:
        if self._store is None:
            raise StoreNotDefinedError("Store must be defined")
        return join_path(*self._relative_path, self._name, key, sep=self._store.sep)

    def add_group(self, name: str) -> "EOGroup":
        relative_path = [*self._relative_path, self.name]
        group = EOGroup(name, self, relative_path=relative_path)
        self[name] = group
        if self._store is not None and self._store.status == StorageStatus.OPEN:
            self._store.add_group(name, relative_path=relative_path)
        return group

    def add_variable(self, name: str, data: Optional[Any] = None, **kwargs) -> EOVariable:
        if self._dataset is None:
            self._dataset = xarray.Dataset()
        self._dataset[name] = xarray.DataArray(name=name, data=data, **kwargs)
        if self._store is not None and self._store.status == StorageStatus.OPEN:
            self._store.add_variables(self._name, self._dataset, relative_path=self._relative_path)
        return EOVariable(name, self._dataset[name], self._product)

    def write(self):
        for name, item in self.groups:
            if name not in self._store[self._path]:
                self._store.add_group(name, relative_path=[*self._relative_path, self._name])
            item.write()
        if self._dataset is not None:
            self._store.add_variables(self._name, self._dataset, relative_path=self._relative_path)


class EOProduct(MutableMapping[str, EOGroup]):
    """"""

    def __init__(self, name: str, store_or_path_url: Optional[Union[str, EOProductStore]] = None):
        self._name: str = name
        self._groups: dict[str, Optional[EOGroup]] = {}
        self._store: Optional[EOProductStore] = None
        self.__set_store(store_or_path_url=store_or_path_url)

    def __set_store(self, store_or_path_url: Optional[Union[str, EOProductStore]] = None):
        from .store.zarr import EOZarrStore

        if isinstance(store_or_path_url, str):
            self._store = EOZarrStore(store_or_path_url)
        elif isinstance(store_or_path_url, EOProductStore):
            self._store = store_or_path_url
        elif store_or_path_url is not None:
            raise TypeError(f"{type(store_or_path_url)} can't be used to instantiate EOProductStore.")

    def __getitem__(self, key: str) -> EOGroup:
        return self._get_group(key)

    def __setitem__(self, key: str, value: EOGroup) -> None:
        self._groups[key] = value

    def __iter__(self) -> Iterator[str]:
        for key in self._store:
            if key not in self._groups:
                yield key
        yield from self._groups

    def __delitem__(self, key: str) -> None:
        if key in self._groups:
            del self._groups[key]
        if self._store and key in self._store:
            del self._store[key]

    def __len__(self) -> int:
        length = len(self._groups)
        if self._store is not None:
            length += len(key for key in self._store if key not in self._groups)
        return length

    def __getattr__(self, attr: str) -> EOGroup:
        return self[attr]

    def _get_group(self, group_name):
        group = self._groups.get(group_name)
        if group is None:
            if self._store is None:
                raise KeyError(f"Invalide EOGroup name: {group_name}")
            name, relative_path, dataset, attrs = self._store[group_name]
            group = EOGroup(name, self, relative_path=relative_path, dataset=dataset, attrs=attrs)
            self[group_name] = group
        return group

    def add_group(self, name: str) -> EOGroup:
        group = EOGroup(name, self, relative_path=[])
        self[name] = group
        if self._store is not None and self._store.status == StorageStatus.OPEN:
            self._store.add_group(name)
        return group

    @property
    def name(self) -> str:
        return self._name

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOProduct]{hex(id(self))}"

    def _repr_html_(self):
        return renderer("product.html", product=self)

    def _ipython_key_completions_(self):
        return self.keys()

    def open(self, store_or_path_url: Optional[Union[EOProductStore, str]] = None, mode: str = "r", **kwargs: Any):
        if store_or_path_url:
            self.__set_store(store_or_path_url=store_or_path_url)
        self._store.open(mode=mode, **kwargs)
        return self

    def load(self):
        ...

    def write(self):
        self.validate()
        for name, group in self._groups.items():
            if name not in self._store:
                self._store.add_group(name)
            group.write()

    def is_valid(self):
        return all(key in self for key in ("measurement", "coordinates", "attributes"))

    def validate(self):
        if not self.is_valid():
            raise InvalidProductError(f"Invalid product {self}, missing mandatory groups.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._store.close()
