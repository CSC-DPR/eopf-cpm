from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from typing import Iterable

from eopf.exceptions import StoreNotDefinedError
from eopf.product.core import EOProduct
from eopf.product.object import EOObject

from .core import EOGroup
from .store import EOProductStore, StorageStatus
from .utils import join_path, parse_path


class EOAbstract(ABC):
    @abstractmethod
    @property
    def product(self) -> EOProduct:
        ...

    @abstractmethod
    @property
    def store(self) -> EOProductStore:
        ...

    @abstractmethod
    @property
    def path(self) -> str:
        ...

    @abstractmethod
    @property
    def relative_path(self) -> Iterable[str]:
        ...

    @abstractmethod
    @property
    def name(self) -> str:
        ...


class EOContainer(EOAbstract, MutableMapping[str, EOObject]):
    def __init__(self):
        self._groups: dict[str, "EOGroup"] = {}

    def __getitem__(self, key: str) -> EOObject:
        return self._get_item(key)

    def _get_item(self, key: str) -> EOObject:
        """find and return eovariable or eogroup from the given key.

        if store is defined and key not already loaded in this group,
        data is loaded from it.
        Parameters
        ----------
        key: str
            name of the eovariable or eogroup
        """

        key, subkey = parse_path(key)
        item: EOGroup
        if key not in self._groups and self.store is None:
            raise KeyError(f"Invalide EOGroup item name {key}")
        elif key not in self._groups and self.store is not None:
            name, relative_path, dataset, attrs = self.store[self._relative_key(key)]
            item = EOGroup(name, self.product, relative_path=relative_path, dataset=dataset, attrs=attrs)
        else:
            item = self._groups[key]
        self[key] = item
        if subkey is not None:
            return item[subkey]
        return item

    def __delitem__(self, key: str) -> None:
        if key in self._groups:
            del self._groups[key]
        if self.store is not None and (store_key := self._relative_key(key)) in self.store:
            del self.store[store_key]

    def __len__(self) -> int:
        keys = set(self._groups)
        if self.store is not None:
            keys |= set(self.store[self.path])
        return len(keys)

    def __getattr__(self, attr: str) -> EOObject:
        if attr in self:
            return self[attr]
        raise AttributeError(attr)

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
        return join_path(*self.relative_path, self.name, key, sep=self.store.sep)

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
        relative_path = [*self.relative_path, self.name]
        name, keys = parse_path(name)
        group = EOGroup(name, self.product, relative_path=relative_path)
        self[name] = group
        if self.store is not None and self.store.status == StorageStatus.OPEN:
            self.store.add_group(name, relative_path=relative_path)

        if keys is not None:
            group = group.add_group(keys)
        return group

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
        for name, item in self._groups.items():
            if name not in self.store.iter(self.path):  # pyre-ignore[16]
                self.store.add_group(name, relative_path=[*self.relative_path, self.name])  # pyre-ignore[16]
            item.write()

    def _ipython_key_completions_(self) -> list[str]:
        return [key for key in self.keys()]

    def __contains__(self, key: object) -> bool:
        return (key in self._groups) or (
            self.store is not None and key in self.store.iter(self.path)
        )  # type: ignore[operator]
