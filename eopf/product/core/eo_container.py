import warnings
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Callable, Iterator, Optional

from eopf.exceptions import EOObjectExistError, StoreNotDefinedError
from eopf.product.core.eo_abstract import EOAbstract
from eopf.product.store import StorageStatus
from eopf.product.utils import (
    downsplit_eo_path,
    is_absolute_eo_path,
    join_path,
    norm_eo_path,
    product_relative_path,
)

if TYPE_CHECKING:  # pragma: no cover
    from .eo_group import EOGroup
    from .eo_object import EOObject
    from .eo_variable import EOVariable


class EOContainer(EOAbstract, MutableMapping[str, "EOObject"]):
    """Abstract class implemented by EOProduct and EOGroup.
    Storage of EOVariable and EOGroup, linked to their EOProduct's EOProductStore (if existing).
    Read and write both dynamically, or on demand to the EOProductStore.
    Can be used in a dictionary like manner with relatives and absolutes paths.

    Parameters
    ----------
    attrs: dict[str, Any], optional
        Attributes to assign
    """

    def __init__(self, attrs: Optional[MutableMapping[str, Any]] = None) -> None:
        self._groups: dict[str, "EOGroup"] = {}
        self._attrs: dict[str, Any] = dict(attrs) if attrs is not None else {}
        self._variables: dict[str, "EOVariable"] = dict()

    def __getitem__(self, key: str) -> "EOObject":
        return self._get_item(key)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        from .eo_group import EOGroup

        if key == "":
            raise KeyError("Invalid key")
        if is_absolute_eo_path(key):
            self.product[product_relative_path(self.path, key)] = value
            return
        # fixme should probably set the product and path
        key, subkeys = downsplit_eo_path(key)
        if subkeys:
            sub_container = self[key]
            if not isinstance(sub_container, EOContainer):  # sub_container is a EOVariable
                raise KeyError("EOVariable not support support item assignment")
            sub_container[subkeys] = value
            return

        if isinstance(value, EOGroup):
            self._add_local_group(key, value)
        else:
            self._add_local_variable(key, value)

    def __iter__(self) -> Iterator[str]:
        yield from self._groups
        yield from self._variables
        if self.store is None:
            return
        if self.store.status == StorageStatus.CLOSE:
            warnings.warn("`for in` statement can't check store")
            return
        for key in self.store.iter(self.path):
            if key not in self._groups and key not in self._variables:
                yield key

    def _get_item(self, key: str) -> "EOObject":
        """find and return eovariable or eogroup from the given key.

        if store is defined and key not already loaded in this group,
        data is loaded from it.

        Parameters
        ----------
        key: str
            name of the eovariable or eogroup

        Returns
        -------
        EOObject
        """
        from .eo_group import EOGroup

        if self.store is not None and self.store.status == StorageStatus.CLOSE:
            warnings.warn("store closed, it will be ignored")

        if is_absolute_eo_path(key):
            return self.product._get_item(product_relative_path(self.path, key))

        key, subkey = downsplit_eo_path(key)

        if subkey is None and key in self._variables:
            return self._variables[key]

        item: "EOObject"
        if key not in self._groups and (self.store is None or self.store.status == StorageStatus.CLOSE):
            raise KeyError(f"Invalide EOGroup item name {key}")
        elif key in self._groups:
            item = self._groups[key]
        elif self.store is not None:
            item = self.store[self._store_key(key)]
            if isinstance(item, EOGroup):
                self._add_local_group(key, item)
            else:
                self._add_local_variable(key, item, new_eo=False)

        if subkey is not None:
            if not isinstance(item, EOGroup):
                raise TypeError
            return item[subkey]
        return item

    def __delitem__(self, key: str) -> None:
        if is_absolute_eo_path(key):
            raise KeyError("__delitem__ can't take an absolute path as argument")
        if key in self._variables:
            del self._variables[key]
            return
        name, keys = downsplit_eo_path(key)

        if keys is None:
            if name in self._groups:
                del self._groups[name]
            if self.store is not None and (store_key := self._store_key(name)) in self.store:
                del self.store[store_key]
        else:
            sub_container = self[name]
            if not isinstance(sub_container, EOContainer):  # sub_container is a EOVariable
                raise KeyError("EOVariable not support item deletion")
            del sub_container[keys]

    def __len__(self) -> int:
        return len(set(self))

    def __getattr__(self, attr: str) -> "EOObject":
        if attr in self:
            return self[attr]
        raise AttributeError(attr)

    def __contains__(self, key: Any) -> bool:
        if self.store is not None and self.store.status == StorageStatus.CLOSE:
            warnings.warn("`in` statement can't check store")
        if is_absolute_eo_path(key):
            raise KeyError("__contains__ can't take an absolute path as argument")
        direct_key, subkey = downsplit_eo_path(key)
        if direct_key in self._variables:
            return subkey is None

        if direct_key in self._groups:
            if subkey is None:
                return True
            else:
                return subkey in self._groups[direct_key]

        return (
            self.store is not None
            and self.store.status == StorageStatus.OPEN
            and any(key == store_key for store_key in self.store.iter(self.path))
        )

    def _store_key(self, key: str) -> str:
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
        from eopf.product import EOProduct

        if isinstance(self, EOProduct):
            name = key
        else:
            name = join_path(self.name, key, sep=self.store.sep)
        return join_path(*self.relative_path, name, sep=self.store.sep)

    def _recursive_add(self, path: str, add_local_method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Recursively got through the path , adding group as needed,
        then add it to the local container using add_local_method

        Parameters
        ----------
        path: str
            path of the new EOObject.
        add_local_method: Callable[..., Any]
            methode used to add locally the new EOObject
        *args: Any
            Positionals arguments of add_local_method additional to the name
        **kwargs: Any
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

    def _add_local_group(self, name: str = "", group: Optional["EOGroup"] = None, **kwargs: Any) -> "EOGroup":
        """Add a group local to the EOContainer. Does not support paths and recursively adding subgroups.

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

        if group is None:
            group = EOGroup(**kwargs)
        if name == "":
            name = group.name
        if name == "":
            raise ValueError("group name can't be empty")
        group._repath(name, self)
        self._groups[name] = group
        return group

    def _add_local_variable(
        self, name: str = "", data: Optional[Any] = None, new_eo: bool = True, **kwargs: Any
    ) -> "EOVariable":  # pragma: no cover
        """Add a variable local to the EOVariable. Does not support paths and recursively adding subgroups.

        The given data is copied to create the new EOVariable object.

        Parameters
        ----------
        name: str
            name of the variable to add
        data: any, optional
            data to use for the variable, should be a type accepted by xarray.DataArray
        **kwargs: any
            extra arguments accepted by :obj:`eopf.product.EOVariable`

        Returns
        -------
        EOVariable
            newly created EOVariable

        Raises
        ------
        EOObjectExistError
            If an object already exist at this path.
        InvalidProductError
            If you store a variable locally to a product.
        """
        from .eo_variable import EOVariable

        if name == "":
            name = getattr(data, "name", "")
        if name == "":
            raise ValueError("variable name can't be empty")
        if new_eo or not isinstance(data, EOVariable):
            variable = EOVariable(name, data, self, **kwargs)
        else:
            data._repath(name, self)
            variable = data
        self._variables[name] = variable

        return variable

    def add_group(
        self,
        name: str,
        attrs: dict[str, Any] = {},
        dims: tuple[str, ...] = tuple(),
    ) -> "EOGroup":
        """Construct and add an EOGroup to this container

        If store is defined and open, the group is directly writen by the store.
        Coordinates should be set in the given key-value pair format: {"path/under/coordinates/group" : array_like}.
        About dimension, if a value is a path under the coordinates group, the coordinates is automatically associated.

        Parameters
        ----------
        name: str
            name of the future group
        attrs: dict[str, Any], optional
            attributes to assign to the new group
        dims: tuple[str, ...], optional
            dimensions to assign

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
            return subcontainer._add_local_group(name, attrs=attrs, dims=dims)

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
        data: any, optional
            data to use for the variable, should be a type accepted by xarray.DataArray
        **kwargs: any
            extra arguments accepted by :obj:`eopf.product.EOVariable`

        Returns
        -------
        EOVariable
            newly created EOVariable

        Raises
        ------
        EOObjectExistError
            If an object already exist at this path.

        See Also
        --------
        xarray.DataArray
        """

        def local_adder(subcontainer: EOContainer, name: str, data: Optional[Any], **kwargs: Any) -> "EOVariable":
            return subcontainer._add_local_variable(name, data, **kwargs)

        if is_absolute_eo_path(name):
            return self.product.add_variable(product_relative_path(self.path, name), data=data, **kwargs)

        # We want the method of the subtype.
        return self._recursive_add(norm_eo_path(name), local_adder, data, **kwargs)

    def write(self) -> None:
        """Write non synchronized subgroups, variables to the store

        the store must be opened to work

        Parameters
        ----------
        erase: bool, optional
            if the data already exist, there should be erase or not

        Raises
        ------
        StoreNotDefinedError
            Trying to write without a store
        StoreNotOpenError
            Trying to write in a closed store

        See Also
        --------
        EOProduct.open
        EOProduct.load
        """
        super().write()
        for name in self:
            self[name].write()

    def load(self) -> None:
        """Load all the product in memory

        The store must be open

        Parameters
        ----------
        erase: bool, optional
            if the data already exist, there should be erase or not

        Raises
        ------
        StoreNotDefinedError
            Trying to read without a store
        StoreNotOpenError
            Trying to read in a closed store

        See Also
        --------
        EOProduct.open
        EOProduct.write
        """
        from .eo_group import EOGroup

        if self.store is None:  # pragma: no cover
            raise StoreNotDefinedError("Store must be defined")
        for key in self:
            eo_object = self[key]
            if isinstance(eo_object, EOGroup):
                eo_object.load()

    @property
    def attrs(self) -> dict[str, Any]:
        """dict[str, Any]: Attributes defined by this object"""
        return self._attrs

    @property
    def groups(self) -> Iterator[tuple[str, "EOGroup"]]:
        """Iterator over the sub EOGroup of this EOGroup"""
        for key, value in self._groups.items():
            yield key, value
        if self.store is not None and self.store.status == StorageStatus.OPEN:
            for key in self.store.iter(self.path):
                path = join_path(self.path, key)
                if key not in self._groups and self.store.is_group(path):
                    yield key, self[path]
        elif self.store is not None and self.store.status == StorageStatus.CLOSE:
            warnings.warn("`for in` statement can't check store")

    @property
    def variables(self) -> Iterator[tuple[str, "EOVariable"]]:
        """Iterator over the couples variable_name, EOVariable of this EOGroup"""
        for key, value in self._variables.items():
            yield key, value
        if self.store is not None and self.store.status == StorageStatus.OPEN:
            for key in self.store.iter(self.path):
                path = join_path(self.path, key)
                if key not in self._variables and self.store.is_variable(path):
                    yield key, self[path]
        elif self.store is not None and self.store.status == StorageStatus.CLOSE:
            warnings.warn("`for in` statement can't check store")

    def _ipython_key_completions_(self) -> list[str]:  # pragma: no cover
        return [key for key in self.keys()]

    @property
    def _is_root(self) -> "bool":
        return False

    def walk(self) -> Iterator["EOObject"]:
        """Iterate over all the internal hierarchy of this EOContainer

        After yielding an EOGroup, all the internal hierarchy of this one
        if yield too.

        Yields
        ------
        EOObject
        """
        from .eo_group import EOGroup

        for value in self.values():
            yield value
            if isinstance(value, EOGroup):
                yield from value.walk()
