from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Hashable, Iterable, Iterator, Optional, Union

import xarray

from eopf.exceptions import StoreNotDefinedError
from eopf.product.core.eo_container import EOContainer
from eopf.product.core.eo_object import EOObject
from eopf.product.utils import join_path

from ..formatting import renderer
from ..store.abstract import StorageStatus
from .eo_variable import EOVariable

if TYPE_CHECKING:
    from eopf.product.core.eo_product import EOProduct


class EOGroup(EOContainer, EOObject, MutableMapping[str, Union[EOVariable, "EOGroup"]]):
    """"""

    def __init__(
        self,
        name: str,
        product: "EOProduct",
        relative_path: Optional[Iterable[str]] = None,
        dataset: Optional[xarray.Dataset] = None,
        attrs: Optional[dict[str, Any]] = None,
    ) -> None:
        EOObject.__init__(self, name, product, relative_path, attrs)

        if dataset is None:
            dataset = xarray.Dataset()
        else:
            for key in dataset:
                if not isinstance(key, str):
                    raise TypeError(f"The dataset key {str(key)} is type {type(key)} instead of str")
            # It is supposed by EOGroup consumer that it's dataset only contain string keys.
        if not isinstance(dataset, xarray.Dataset):
            raise TypeError("dataset parameters must be a xarray.Dataset instance")

        self._dataset = dataset
        self._items: dict[str, "EOGroup"] = {}

    def __getitem__(self, key: str) -> Union[EOVariable, "EOGroup"]:
        return self._get_item(key)

    def _get_item(self, key: str) -> Union[EOVariable, "EOGroup"]:
        """find and return eovariable or eogroup from the given key.

        if store is defined and key not already loaded in this group,
        data is loaded from it.
        Parameters
        ----------
        key: str
            name of the eovariable or eogroup
        """
        if key in self._dataset:
            return EOVariable(key, self._dataset[key], self.product, relative_path=[*self._relative_path, self._name])

        subkey = None
        if "/" in key:
            key, _, subkey = key.partition("/")

        item: EOGroup
        if key not in self._items and self.store is None:
            raise KeyError(f"Invalide EOGroup item name {key}")
        elif key not in self._items and self.store is not None:
            name, relative_path, dataset, attrs = self.store[self._relative_key(key)]
            item = EOGroup(name, self.product, relative_path=relative_path, dataset=dataset, attrs=attrs)
        else:
            item = self._items[key]
        self[key] = item
        if subkey is not None:
            return item[subkey]
        return item

    def __setitem__(self, key: str, value: Union[EOVariable, "EOGroup"]) -> None:
        if isinstance(value, EOGroup):
            self._items[key] = value
        elif isinstance(value, EOVariable):
            self._dataset[value.name] = value
        else:
            raise TypeError(f"Item assigment Impossible for type {type(value)}")

    def __delitem__(self, key: str) -> None:
        if key in self._items:
            del self._items[key]
        if key in self._dataset:
            del self._dataset[key]
        if self.store is not None and (store_key := self._relative_key(key)) in self.store:
            del self.store[store_key]

    def __iter__(self) -> Iterator[str]:
        if self.store is not None:
            for key in self.store.iter(self.path):  # pyre-ignore[16]
                if key not in self._items and key not in self._dataset:
                    yield key
        if self._dataset is not None:
            yield from self._dataset  # type: ignore[misc]
        yield from self._items

    def __len__(self) -> int:
        keys = set(self._items)
        if self.store is not None:
            keys |= set(self.store[self.path])
        return len(keys)

    def __getattr__(self, attr: str) -> Union[EOVariable, "EOGroup"]:
        if attr in self:
            return self[attr]
        raise AttributeError(attr)

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOGroup]{hex(id(self))}"

    def _repr_html_(self) -> str:
        return renderer("group.html", group=self)

    def to_product(self) -> "EOProduct":
        """Convert this group to a product"""
        ...

    @property
    def dims(self) -> tuple[Hashable, ...]:
        """dimension of this group"""
        return tuple(self._dataset.dims)

    @property
    def groups(self) -> Iterator[tuple[str, "EOGroup"]]:
        """iterator over the groups of this group"""
        for key, value in self.items():
            if isinstance(value, EOGroup):
                yield key, value

    @property
    def variables(self) -> Iterator[tuple[str, EOVariable]]:
        """iterator over the variables of this group"""
        for key, value in self.items():
            if isinstance(value, EOVariable):
                yield key, value

    def _relative_key(self, key: str) -> str:
        """helper to construct path of sub object

        Parameters
        ----------
        key: str
            sub object name
        Returns
        -------
        str
            path value with store based separator
        """
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        return join_path(*self._relative_path, self._name, key, sep=self.store.sep)

    def add_group(self, name: str) -> "EOGroup":
        """Construct and add a eogroup to this group

        if store is defined and open, the group it's directly write by the store.
        Parameters
        ----------
        name: str
            name of the future group
        Returns
        -------
        EOGroup
            newly created EOGroup
        """
        relative_path = [*self._relative_path, self.name]
        keys = None
        if "/" in name:
            name, _, keys = name.partition("/")
        group = EOGroup(name, self.product, relative_path=relative_path)
        self[name] = group
        if self.store is not None and self.store.status == StorageStatus.OPEN:
            self.store.add_group(name, relative_path=relative_path)

        if keys is not None:
            group = self[name].add_group(keys)  # type:ignore[union-attr]
            # We juste created it, type shouldn't have changed.
        return group

    def add_variable(self, name: str, data: Optional[Any] = None, **kwargs: Any) -> EOVariable:
        """Construct and add an eovariable to this group

        if store is defined and open, the eovariable it's directly write by the store.
        Parameters
        ----------
        name: str
            name of the future group
        data: any, optional
            data like object use to create dataarray or dataarray
        **kwargs: any
            DataArray extra parameters if data is not a dataarray
        Returns
        -------
        EOVariable
            newly created EOVariable
        """
        variable = EOVariable(name, data, self.product, relative_path=[*self._relative_path, self._name], **kwargs)
        self._dataset[name] = variable._data
        if self.store is not None and self.store.status == StorageStatus.OPEN:
            self.store.add_variables(self._name, self._dataset, relative_path=self._relative_path)
        return variable

    def write(self) -> None:
        """
        write non synchronized subgroups, variables to the store

        the store must be opened to work
        See Also
        --------
        EOProduct.open
        """
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        for name, item in self.groups:
            if name not in self.store.iter(self.path):  # pyre-ignore[16]
                self.store.add_group(name, relative_path=[*self._relative_path, self._name])  # pyre-ignore[16]
            item.write()
        if self._dataset is not None and len(self._dataset) > 0:
            self.store.add_variables(self._name, self._dataset, relative_path=self._relative_path)  # pyre-ignore[16]

    def _ipython_key_completions_(self) -> list[str]:
        return [key for key in self.keys()]

    def __contains__(self, key: object) -> bool:
        return (
            (key in self._items)
            or (key in self._dataset)
            or (self.store is not None and key in self.store.iter(self.path))  # type: ignore[operator]
        )
