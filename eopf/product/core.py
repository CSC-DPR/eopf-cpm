"""
Define core object compliant to the common data model
and the Climat and Forecast convention.

common data model: https://www.unidata.ucar.edu/software/netcdf/docs/netcdf_data_model.html#enhanced_model
Climate and Forecast conventions: https://cfconventions.org/
"""

from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from typing import Any, Iterable, Iterator, KeysView, List, Optional, Union

from xarray import DataArray

from eopf import exceptions


def search(eoobject: Union["EOGroup", "EOProduct"], path, already_done: List["EOGroup"] = None):
    """
    Climate and forecast convention search mechanisme for group
    https://cfconventions.org/Data/cf-conventions/cf-conventions-1.9/cf-conventions.html#_scope
    """
    already_done = already_done or []
    already_done.append(eoobject)

    root, _, subpaths = path.partition("/")
    if not root:  # absolute path
        if not isinstance(eoobject, EOProduct):
            parent = eoobject.parent
            while parent.parent is not None:
                parent = parent.parent
        else:
            parent = eoobject
        return parent.search(subpaths)
    elif root and subpaths:  # relative path
        current_item = eoobject.groups.get(root)
        if current_item:
            return current_item.search(subpaths)
        return current_item
    else:  # proximity
        current_item = eoobject.get(root)
        if current_item:
            return current_item
        for group in eoobject.parent.groups.values():
            if group not in already_done:
                find = group.search(root, already_done=already_done)
                if find:
                    return find
    return


class EOProperties(ABC):
    """Common properties of the Common data model."""

    @property
    @abstractmethod
    def name(self) -> Optional[str]:
        """
        Accessor to the name of the EO Object.
        """

    @property
    @abstractmethod
    def dims(self) -> tuple[str]:
        """
        Accessor to the dims of the EO Object.
        Compliant to the Common data model.
        """

    @property
    @abstractmethod
    def attrs(self) -> MutableMapping[str, Any]:
        """
        Accessor to the attributes.
        Compliant to the Common data model.
        """

    @property
    @abstractmethod
    def parent(self) -> Union["EOProduct", "EOGroup", None]:
        """
        Accessor the the direct parent.
        Compliant to the Common data model.
        """


class EOVariable(EOProperties):
    """Earth Observation Variable definition
    Compliant to the Common data model.
    Compliant to the NEP 13 and 18.
    ndarray must follow the linear algebra ext:
    - https://data-apis.org/array-api/latest/extensions/linear_algebra_functions.html

    Using dask as graph processing management:
    - https://docs.dask.org/en/stable/custom-collections.html
    """

    __slots__ = (
        "_ndarray",
        "_parent",
    )

    def __types__(self) -> None:
        self._ndarray: DataArray
        self._parent: Optional["EOGroup"] = None

    def __init__(self, ndarray: DataArray):
        self.__types__()
        self._ndarray = ndarray

    def __str__(self):
        return self.__repr__()

    def __repr__(self) -> str:
        parent_part = f"(parent={self.parent.name})" if self._parent else ""
        return f"[EOGVariable]{parent_part}(dims={self.dims}) {self.name}"

    @property
    def attrs(self) -> MutableMapping[str, Any]:
        return self._ndarray.attrs

    @property
    def dims(self) -> tuple[str]:
        return self._ndarray.dims

    @property
    def name(self) -> Optional[str]:
        return self._ndarray.name

    @property
    def parent(self) -> "EOGroup":
        if self._parent is None:
            raise exceptions.InitializeError("parent of the current EOVariable is not set.")
        return self._parent

    @parent.setter
    def parent(self, value: "EOGroup"):
        if not isinstance(value, EOGroup):
            raise ValueError(
                f"parent of EOVariable instance must be EOGroup instance, but your have send {type(value)} instance.",
            )
        self._parent = value

    # @abstractmethod
    # def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
    #     """Must be compliant to the NEP 13.
    #     https://numpy.org/neps/nep-0013-ufunc-overrides.html
    #     """

    # @abstractmethod
    # def __array_function__(self, func, types, args, kwargs):
    #     """Must be compliant to th NEP 18.
    #     https://numpy.org/neps/nep-0018-array-function-protocol.html
    #     """

    # @abstractmethod
    # def __dask_graph__(self) -> Union[Mapping, None]:
    #     """ """

    # @abstractmethod
    # def __dask_keys__(self) -> list:
    #     """ """

    # @abstractmethod
    # def __dask_layers__(self) -> tuple:
    #     """ """

    # @abstractmethod
    # def __dask_postcompute__(self) -> tuple[Callable, tuple]:
    #     """ """

    # @abstractmethod
    # def __dask_postpersist__(self) -> tuple[Callable, tuple]:
    #     """ """

    # @abstractmethod
    # def __dask_tokenize__(self) -> Hashable:
    #     """Dask deterministic Hashing:
    #     https://docs.dask.org/en/stable/custom-collections.html#deterministic-hashing"""

    # @staticmethod
    # @abstractmethod
    # def __dask_scheduler__(dsk, keys, **kwargs):
    #     """ """

    # @property
    # @abstractmethod
    # def chunks(self) -> Optional[tuple[tuple[int, ...], ...]]:
    #     """Accessor to the chunks"""

    # @abstractmethod
    # def rechunk(
    #     self,
    #     chunks_shape: Union[
    #         Mapping[Any, Union[None, int, tuple[int, ...]]],
    #         int,
    #         tuple[int, ...],
    #         tuple[tuple[int, ...], ...],
    #     ],
    # ) -> None:
    #     """Change chunk shape / size"""

    # @abstractmethod
    # def map_chunk(self, func):
    #     """map function on each chunk"""


class EOGroup(EOProperties, MutableMapping[str, EOVariable]):
    """Earth Observation Group definition
    Compliant to the Common data model and
    Climate and forecast conventions.
    """

    __slots__ = (
        "_name",
        "_variables",
        "_groups",
        "_attrs",
        "_dims",
        "_coords",
        "_parent",
    )

    def __types__(self) -> None:
        self._name: str
        self._variables: MutableMapping[str, EOVariable]
        self._groups: MutableMapping[str, "EOGroup"]
        self._attrs: MutableMapping[str, Any]
        self._dims: tuple[str]
        self._coords: Optional["EOGroup"] = None
        self._parent: Optional[Union["EOGroup", "EOProduct"]] = None

    def __init__(
        self,
        name: str,
        *args: EOVariable,
        variables: Optional[Iterable[EOVariable]] = None,
        groups: Optional[Iterable["EOGroup"]] = None,
        coords: Optional["EOGroup"] = None,
        attrs: Optional[MutableMapping[str, Any]] = None,
        dims: Optional[tuple[str]] = None,
    ) -> None:
        self.__types__()
        self._name = name
        variables = [*args, *(variables or [])]
        self._variables = {}
        for variable in variables:
            self[variable.name] = variable

        self._groups = {group.name: group for group in (groups or [])}
        if coords:
            self._coords = coords
            self._coords.parent = self

        self._attrs = {}
        self._attrs.update(attrs or {})

        self._dims = tuple(i for i in (dims or []))

    def __getitem__(self, key: str) -> EOVariable:
        return self._variables.__getitem__(key)

    def __setitem__(self, key: str, value: EOVariable) -> None:
        self._variables.__setitem__(key, value)
        value.parent = self

    def __delitem__(self, key: str) -> None:
        self._variables.__delitem__[key]

    def __len__(self) -> int:
        return self._variables.__len__()

    def __iter__(self) -> Iterator[EOVariable]:
        return self._variables.__iter__()

    def __str__(self):
        return self.__repr__()

    def __repr__(self) -> str:
        parent_part = f"(parent={self.parent.name or type(self.parent)})" if self._parent else ""
        return f"[EOGroup]{parent_part}(dims={self.dims}) {self.name}"

    @property
    def attrs(self):
        return self._attrs

    @property
    def coords(self) -> "EOGroup":
        return self._coords

    @property
    def dims(self) -> tuple[str]:
        return self._dims

    @property
    def groups(self) -> MutableMapping[str, "EOGroup"]:
        return self._groups

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def parent(self):
        if self._parent is None:
            raise exceptions.InitializeError("parent of the current EOGroup is not set.")
        return self._parent

    @parent.setter
    def parent(self, value: Union["EOGroup", "EOProduct"]):
        if not isinstance(value, (EOGroup, EOProduct)):
            raise ValueError("parent of EOGroup instance must be EOGroup or EOProduct instance")
        self._parent = value

    def keys(self) -> KeysView["EOGroup"]:
        return self._variables.keys()

    def values(self) -> KeysView["EOGroup"]:
        return self._variables.values()


class MetaData(ABC):
    @property
    @abstractmethod
    def path(self):
        """ """

    @property
    @abstractmethod
    def store(self):
        """ """


class EOProduct(EOProperties, MutableMapping[str, EOGroup]):
    """Earth Observation Top Group definition
    Compliant to the Common data model.
    """

    __slots__ = ("_name", "_groups", "_coords", "_attrs", "_metadata")

    def __types__(self):
        self._name: str
        self._groups: MutableMapping[str, EOGroup]
        self._coords: EOGroup
        self._attrs: MutableMapping[str, Any]
        self._metadata: Optional[MetaData] = None

    def __init__(
        self,
        name: str,
        coords: Optional[EOGroup],
        *args: EOGroup,
        groups: Optional[Iterable[EOGroup]] = None,
        attrs: Optional[MutableMapping[str, Any]] = None,
        metadata: Optional[MetaData] = None,
    ) -> None:
        self.__types__()
        self._name = name

        self._coords = coords
        self._coords.parent = self

        groups = [*args, *(groups or [])]
        self._groups = {}
        for group in groups:
            self[group.name] = group

        self._attrs = {}
        self._attrs.update(attrs or {})

        self._metadata = metadata

    def __getitem__(self, key: str) -> EOGroup:
        return self._groups.__getitem__(key)

    def __setitem__(self, key: str, value: EOGroup) -> None:
        self._groups.__setitem__(key, value)
        value.parent = self

    def __delitem__(self, key: str) -> None:
        self._groups.__delitem__[key]

    def __len__(self) -> int:
        return self._groups.__len__()

    def __iter__(self) -> Iterator[EOGroup]:
        return self._groups.__iter__()

    def __str__(self):
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOProduct](metadata={self.metadata}) {hex(id(self))}"

    @property
    def attrs(self) -> MutableMapping[str, Any]:
        return self._attrs

    @property
    def coords(self) -> EOGroup:
        return self._coords

    @property
    def dims(self) -> tuple[str]:
        return tuple()

    @property
    def metadata(self) -> Optional[MetaData]:
        return self._metadata

    @property
    def name(self) -> str:
        return self._name

    @property
    def parent(self) -> None:
        return None

    def keys(self) -> KeysView["EOProduct"]:
        return self._groups.keys()

    def values(self) -> KeysView["EOProduct"]:
        return self._groups.values()
