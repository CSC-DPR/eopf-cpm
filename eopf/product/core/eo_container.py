from abc import abstractmethod
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Callable, Iterator, Optional, Union

from eopf.exceptions import GroupExistError, StoreNotDefinedError
from eopf.product.core.eo_abstract import EOAbstract
from eopf.product.store import StorageStatus
from eopf.product.utils import join_path, parse_path, split_path

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
            keys |= set(_ for _ in self.store.iter(self.path))
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
        return join_path(self.path, key, sep=self.store.sep)

    def add_group(self, name: str) -> "EOGroup":
        """Construct and add an EOGroup to this container

        if store is defined and open, the group is directly writen by the store.
        Parameters
        ----------
        name: str
            name of the future group
        Returns
        -------
        EOGroup
            newly created EOGroup
        """

        def local_adder(subcontainer: EOContainer, name: str) -> "EOGroup":
            return subcontainer._add_local_group(name)

        # We want the method of the subtype.
        return self._recursive_add(name, local_adder)

    def add_variable(self, name: str, data: Optional[Any] = None, **kwargs: Any) -> "EOGroup":
        """Construct and add an EOVariable to this container

        if store is defined and open, the variable is directly writen by the store.
        Parameters
        ----------
        name: str
            name of the future variable
        Returns
        -------
        EOVariable
            newly created EOVariable
        """

        def local_adder(subcontainer: EOContainer, name: str, data: Optional[Any], **kwargs: Any) -> "EOVariable":
            return subcontainer._add_local_variable(name, data, **kwargs)

        # We want the method of the subtype.
        return self._recursive_add(name, local_adder, data, **kwargs)

    def _recursive_add(self, name: str, add_local_method: Callable[..., Any], *argc: Any, **argv: Any) -> Any:
        name, keys = parse_path(name)

        if keys is None:
            if name in self:
                raise GroupExistError(f"Object {name} already exist in {self.name}")
            return add_local_method(self, name, *argc, **argv)
        else:
            group = self._add_local_group(name)
            return group._recursive_add(keys, add_local_method, *argc, **argv)

    def _add_local_group(self, name: str) -> "EOGroup":
        from .eo_group import EOGroup

        relative_path = split_path(self.path, "/")
        group = EOGroup(name, self.product, relative_path=relative_path)
        self[name] = group
        if self.store is not None and self.store.status == StorageStatus.OPEN:
            self.store.add_group(name, relative_path=relative_path)
        return group

    @abstractmethod
    def _add_local_variable(self, name: str, data: Optional[Any] = None, **kwargs: Any) -> "EOVariable":
        ...

    def write(self, erase: bool = False) -> None:
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
            if not erase and name in self.store.iter(self.path):
                continue
            self.store.add_group(name, relative_path=[*self.relative_path, self.name])
            item.write(erase=erase)

    def load(self, erase: bool = False) -> None:
        """load all the product in memory"""
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        for key in self.store.iter(self.path):
            if not erase and key in self._groups:
                continue
            name, relative_path, dataset, attrs = self.store[self._relative_key(key)]
            group = EOGroup(name, self.product, relative_path=relative_path, dataset=dataset, attrs=attrs)
            self._groups[key] = group
            group.load(erase=erase)

    def _ipython_key_completions_(self) -> list[str]:
        return [key for key in self.keys()]
