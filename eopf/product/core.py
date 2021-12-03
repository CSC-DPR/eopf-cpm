from abc import ABC
from collections.abc import MutableMapping
from typing import Generic, Iterable, List, Optional, TypeVar

from xarray import DataArray


class EOObject:
    name: str
    _parent: "EOObject" = None


T = TypeVar("T", bound="EOObject")


class EOMapping(MutableMapping, Generic[T]):
    _items: MutableMapping[str, T]

    def __init__(self, items: MutableMapping[str, T] = None, **kwargs) -> None:
        self.update(items or {})
        self.update(**kwargs)

    def __getitem__(self, key: str) -> T:
        return self._items[key]

    def __setitem__(self, key: str, value: T) -> None:
        self._items[key] = value

    def __delitem__(self, key: str) -> None:
        del self._items[key]

    def __iter__(self) -> Iterable[T]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)


class EOVariable(EOObject):
    _ndarray: DataArray


class EOGroup(ABC, EOObject, EOMapping[EOVariable]):
    """ """

    _childgroups: EOMapping["EOGroup"]

    @property
    def parent(self) -> "EOGroup":
        return self._parent

    @parent.setter
    def parent(self, parent: "EOGroup") -> None:
        if not isinstance(parent, EOGroup):
            raise TypeError("parent attribute must be a subclass of EOGroup")
        self._parent = parent

    @property
    def groups(self) -> EOMapping["EOGroup"]:
        return self._childgroups

    @property
    def variables(self) -> MutableMapping[str, EOVariable]:
        return self._items

    def append_subgroup(self, childgroup):
        self.groups[childgroup.name] = childgroup

    def search(self, path, already_done: List["EOGroup"] = None):
        already_done = already_done or []
        already_done.append(self)

        root, _, subpaths = path.partition("/")
        if not root:  # absolute path
            parent = self.parent
            while parent.parent is not None:
                parent = parent.parent
            return parent.search(subpaths)
        elif root and subpaths:  # relative path
            current_item = self.groups.get(root)
            if current_item:
                return current_item.search(subpaths)
            return current_item
        else:  # proximity
            current_item = self.variables.get(root)
            if current_item:
                return current_item
            for group in self.parent.groups.values():
                if group not in already_done:
                    find = group.search(root, already_done=already_done)
                    if find:
                        return find
        return


class MetaData:
    pass


class EOProduct(ABC, EOMapping[EOGroup]):
    _metadatas: Optional[MetaData] = None

    @property
    def groups(self) -> EOGroup:
        return self._items
