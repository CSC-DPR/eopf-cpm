import warnings
from abc import abstractmethod
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Callable, Hashable, Iterator, Optional, Union

from eopf.exceptions import EOObjectExistError, StoreNotDefinedError
from eopf.product.core.eo_abstract import EOAbstract
from eopf.product.store import StorageStatus
from eopf.product.utils import (
    downsplit_eo_path,
    is_absolute_eo_path,
    join_path,
    norm_eo_path,
    partition_eo_path,
    product_relative_path,
)

if TYPE_CHECKING:  # pragma: no cover
    from .eo_group import EOGroup
    from .eo_variable import EOVariable


class EOContainer(EOAbstract, MutableMapping[str, Union["EOGroup", "EOVariable"]]):
    """
    Abstract class implemented by EOProduct and EOGroup.
    Storage of EOVariable and EOGroup, linked to their EOProduct's EOProductStore (if existing).
    Read and write both dynamically, or on demand to the EOProductStore.
    Can be used in a dictionary like manner with relatives and absolutes paths.
    """

    def __init__(self, attrs: Optional[dict[Hashable, Any]] = None) -> None:
        self._groups: dict[str, "EOGroup"] = {}
        self._attrs: dict[Hashable, Any] = attrs or dict()

    def __getitem__(self, key: str) -> Union["EOGroup", "EOVariable"]:
        return self._get_item(key)

    def __setitem__(self, key: str, value: Union["EOGroup", "EOVariable"]) -> None:
        from .eo_group import EOGroup

        if key == "":
            raise KeyError("Invalid key")
        if is_absolute_eo_path(key):
            self.product[product_relative_path(self.path, key)] = value
            return
        # fixme should probably set the product and path
        key, subkeys = downsplit_eo_path(key)
        if subkeys:
            self[key][subkeys] = value
            return
        item_rel_path = partition_eo_path(self.path)

        value._repath(key, self.product, item_rel_path)
        if isinstance(value, EOGroup):
            self._groups[key] = value
        else:
            self._add_local_variable(key, value)

    def __iter__(self) -> Iterator[str]:
        if self.store is not None and self.store.status == StorageStatus.CLOSE:
            warnings.warn("`for in` statement can't check store")
        elif self.store is not None:
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
        from .eo_group import EOGroup

        if self.store is not None and self.store.status == StorageStatus.CLOSE:
            warnings.warn("store close, it will be ignore")

        if is_absolute_eo_path(key):
            return self.product._get_item(product_relative_path(self.path, key))

        key, subkey = downsplit_eo_path(key)
        item: EOGroup

        if key not in self._groups and (self.store is None or self.store.status == StorageStatus.CLOSE):
            raise KeyError(f"Invalide EOGroup item name {key}")
        elif key in self._groups:
            item = self._groups[key]
        elif self.store is not None:
            dataset, attrs = self.store.get_data(self._relative_key(key))
            item = EOGroup(dataset=dataset, attrs=attrs)
        self[key] = item
        if subkey is not None:
            return item[subkey]
        return item

    def __delitem__(self, key: str) -> None:
        if is_absolute_eo_path(key):
            raise KeyError("__delitem__ can't take an absolute path as argument")
        name, keys = downsplit_eo_path(key)

        if keys is None:
            if name in self._groups:
                del self._groups[name]
            if self.store is not None and (store_key := self._relative_key(name)) in self.store:
                del self.store[store_key]
        else:
            del self[name][keys]

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
        if self.store is not None and self.store.status == StorageStatus.CLOSE:
            warnings.warn("`in` statement can't check store")
        return (key in self._groups) or (
            self.store is not None
            and self.store.status == StorageStatus.OPEN
            and any(key == store_key for store_key in self.store.iter(self.path))
        )

    def _relative_key(self, key: str) -> str:
        """Helper to construct a store specific path of a sub object.

        Parameters
        ----------
        key: str
            sub object name
        Returns
        -------
        str
            path value with store based separator
        Raises
        ------
        StoreNotDefinedError
            If this object doesn't have a Store.
        """
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        return join_path(self.path, key, sep=self.store.sep)

    def add_group(
        self,
        name: str,
        attrs: dict[Hashable, Any] = {},
        coords: MutableMapping[str, Any] = {},
        dims: tuple[str, ...] = tuple(),
    ) -> "EOGroup":
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
        Raises
        ------
        EOObjectExistError
            If an object already exist at this path.
        """

        def local_adder(subcontainer: EOContainer, name: str) -> "EOGroup":
            return subcontainer._add_local_group(name, attrs=attrs, coords=coords, dims=dims)

        if is_absolute_eo_path(name):
            return self.product.add_group(product_relative_path(self.path, name))

        # We want the method of the subtype.
        return self._recursive_add(norm_eo_path(name), local_adder)

    def add_variable(self, name: str, data: Optional[Any] = None, **kwargs: Any) -> "EOVariable":
        """Construct and add an EOVariable to this container.

        If store is defined and open, the variable is directly writen by the store.
        Parameters
        ----------
        name: str
            name of the future variable
        Returns
        -------
        EOVariable
            newly created EOVariable
        Raises
        ------
        EOObjectExistError
            If an object already exist at this path.
        """

        def local_adder(subcontainer: EOContainer, name: str, data: Optional[Any], **kwargs: Any) -> "EOVariable":
            return subcontainer._add_local_variable(name, data, **kwargs)

        if is_absolute_eo_path(name):
            return self.product.add_variable(product_relative_path(self.path, name), data=data, **kwargs)

        # We want the method of the subtype.
        return self._recursive_add(norm_eo_path(name), local_adder, data, **kwargs)

    def _recursive_add(self, path: str, add_local_method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Recursively got through the path , adding group as needed,
        then add it to the local container using add_local_method

        Parameters
        ----------
        path: str
            path of the new EOObject.
        add_local_method: Callable[..., Any]
            methode used to add locally the new EOObject
        *argc: Any
            Positionals arguments of add_local_method additional to the name
        **argv
            Named arguments of add_local_method additional to the name

        Returns
        -------
        EOObject
            newly created EOObject.
        Raises
        ------
        EOObjectExistError
            If an object already exist at this path.
        """
        path, keys = downsplit_eo_path(path)

        if keys is None:
            if path in self:
                raise EOObjectExistError(f"Object {path} already exist in {self.name}")
            return add_local_method(self, path, *args, **kwargs)

        if path in self._groups:
            group = self._groups[path]
        else:
            group = self._add_local_group(path)
        return group._recursive_add(keys, add_local_method, *args, **kwargs)

    def _add_local_group(
        self,
        name: str,
        attrs: dict[Hashable, Any] = {},
        coords: MutableMapping[str, Any] = {},
        dims: tuple[str, ...] = tuple(),
    ) -> "EOGroup":
        """
        Add a group local to the EOContainer. Does not support paths and recursively adding subgroups.
        Parameters
        ----------
        name:
            Name of the subgroup to add.

        Returns
        -------
        EOGroup
            newly created EOGroup
        Raises
        ------
        EOObjectExistError
            If an object already exist at this path.

        """

        from .eo_group import EOGroup

        group = self[name] = EOGroup(attrs=attrs)
        group.assign_coords(coords)
        group.assign_dims(dims)
        if self.store is not None and self.store.status == StorageStatus.OPEN:
            self.store.add_group(name, relative_path=group.relative_path)
        return group

    @abstractmethod
    def _add_local_variable(
        self, name: str, data: Optional[Any] = None, **kwargs: Any
    ) -> "EOVariable":  # pragma: no cover
        """
        Add a variable local to the EOVariable. Does not support paths and recursively adding subgroups.
        Parameters
        ----------
        name:
            Name of the variable to add.

        Returns
        -------
        EOVariable
            newly created EOVariable
        data: any, optional
            data like object from which the EOVariable dataarray is filled
        **kwargs: any
            DataArray extra parameters if data is not a dataarray
        Raises
        ------
        EOObjectExistError
            If an object already exist at this path.
        InvalidProductError
            If you store a variable locally to a product.
        """
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

    @property
    def attrs(self) -> dict[Hashable, Any]:
        """Attributes defined by this object (does not consider inheritance)."""
        return self._attrs
