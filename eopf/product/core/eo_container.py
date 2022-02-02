from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Iterator, Union

from eopf.exceptions import StoreNotDefinedError
from eopf.product.core.eo_abstract import EOAbstract
from eopf.product.store import StorageStatus
from eopf.product.utils import join_path, parse_path

if TYPE_CHECKING:
    from .eo_group import EOGroup
    from .eo_variable import EOVariable


class EOContainer(EOAbstract, MutableMapping[str, Union["EOGroup", "EOVariable"]]):
    def __init__(self) -> None:
        self._groups: dict[str, "EOGroup"] = {}

    def __getitem__(self, key: str) -> Union["EOGroup", "EOVariable"]:
        return self._get_item(key)

    def __setitem__(self, key: str, value: Union["EOGroup", "EOVariable"]) -> None:
        from .eo_group import EOGroup

        key, subkeys = parse_path(key)
        if subkeys:
            self[key][subkeys] = value
            return

        if isinstance(value, EOGroup):
            self._groups[key] = value
        else:
            raise TypeError(f"Item assigment Impossible for type {type(value)}")

    def __iter__(self) -> Iterator[str]:
        if self.store is not None:
            for key in self.store.iter(self.path):
                if key not in self._groups:
                    yield key
        yield from self._groups

    def _get_item(self, key: str) -> Union["EOGroup", "EOVariable"]:
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

    def __getattr__(self, attr: str) -> Union["EOGroup", "EOVariable"]:
        if attr in self:
            return self[attr]
        raise AttributeError(attr)

    def __contains__(self, key: object) -> bool:
        return (key in self._groups) or (
            self.store is not None and any(key == store_key for store_key in self.store.iter(self.path))
        )

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
        from .eo_group import EOGroup

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
            if name not in self.store.iter(self.path):
                self.store.add_group(name, relative_path=[*self.relative_path, self.name])
            item.write()

    def _ipython_key_completions_(self) -> list[str]:
        return [key for key in self.keys()]
