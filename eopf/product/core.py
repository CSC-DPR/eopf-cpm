"""
Define core object compliant to the common data model
and the Climat and Forecast convention.

common data model: https://www.unidata.ucar.edu/software/netcdf/docs/netcdf_data_model.html#enhanced_model
Climate and Forecast conventions: https://cfconventions.org/
"""

from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from typing import Any, Callable, Hashable, Iterator, List, Mapping, Optional, Union

from xarray import DataArray


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

    @parent.setter
    @abstractmethod
    def parent(self, parent: Union["EOProduct", "EOGroup"]) -> None:
        """
        Set the direct parent of an EO Object.
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

    _ndarray: DataArray

    @abstractmethod
    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        """Must be compliant to the NEP 13.
        https://numpy.org/neps/nep-0013-ufunc-overrides.html
        """

    @abstractmethod
    def __array_function__(self, func, types, args, kwargs):
        """Must be compliant to th NEP 18.
        https://numpy.org/neps/nep-0018-array-function-protocol.html
        """

    @abstractmethod
    def __dask_graph__(self) -> Union[Mapping, None]:
        """ """

    @abstractmethod
    def __dask_keys__(self) -> list:
        """ """

    @abstractmethod
    def __dask_layers__(self) -> tuple:
        """ """

    @abstractmethod
    def __dask_postcompute__(self) -> tuple[Callable, tuple]:
        """ """

    @abstractmethod
    def __dask_postpersist__(self) -> tuple[Callable, tuple]:
        """ """

    @abstractmethod
    def __dask_tokenize__(self) -> Hashable:
        """Dask deterministic Hashing:
        https://docs.dask.org/en/stable/custom-collections.html#deterministic-hashing"""

    @staticmethod
    @abstractmethod
    def __dask_optimize__(dsk, keys, **kwargs) -> MutableMapping:
        """ """

    @staticmethod
    @abstractmethod
    def __dask_scheduler__(dsk, keys, **kwargs):
        """ """

    @property
    @abstractmethod
    def chunks(self) -> Optional[tuple[tuple[int, ...], ...]]:
        """Accessor to the chunks"""

    @abstractmethod
    def rechunk(
        self,
        chunks_shape: Union[
            Mapping[Any, Union[None, int, tuple[int, ...]]],
            int,
            tuple[int, ...],
            tuple[tuple[int, ...], ...],
        ],
    ) -> None:
        """Change chunk shape / size"""

    @abstractmethod
    def map_chunk(self, func):
        """map function on each chunk"""


class EOGroup(EOProperties, MutableMapping[EOVariable]):
    """Earth Observation Group definition
    Compliant to the Common data model and
    Climate and forecast conventions.
    """

    _variables: MutableMapping[EOVariable]

    @property
    @abstractmethod
    def groups(self) -> MutableMapping["EOGroup"]:
        """Earth Obsetvation Sub Groups"""

    def search(self, path, already_done: List["EOGroup"] = None):
        """
        Climate and forecast convention search mechanisme for group
        https://cfconventions.org/Data/cf-conventions/cf-conventions-1.9/cf-conventions.html#_scope
        """
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
            current_item = self.get(root)
            if current_item:
                return current_item
            for group in self.parent.groups.values():
                if group not in already_done:
                    find = group.search(root, already_done=already_done)
                    if find:
                        return find
        return

    def __getitem__(self, key: str) -> EOVariable:
        return self._variables.__getitem__(key)

    def __setitem__(self, key: str, value: EOVariable) -> None:
        self._variables.__setitem__(key, value)

    def __delitem__(self, key: str) -> None:
        self._variables.__delitem__[key]

    def __len__(self) -> int:
        return self._variables.__len__()

    def __iter__(self) -> Iterator[EOVariable]:
        return self._variables.__iter__()


class MetaData(ABC):
    path: Optional[str]

    @property
    @abstractmethod
    def store(self):
        """ """


class EOProduct(EOProperties, MutableMapping[EOGroup]):
    """Earth Observation Top Group definition
    Compliant to the Common data model.
    """

    _groups: MutableMapping[EOGroup]

    @property
    def name(self) -> None:
        return None

    @property
    def parent(self) -> None:
        return None

    @property
    @abstractmethod
    def metadata(self) -> MetaData:
        """ """

    def __getitem__(self, key: str) -> EOVariable:
        return self._groups.__getitem__(key)

    def __setitem__(self, key: str, value: EOVariable) -> None:
        self._groups.__setitem__(key, value)

    def __delitem__(self, key: str) -> None:
        self._groups.__delitem__[key]

    def __len__(self) -> int:
        return self._groups.__len__()

    def __iter__(self) -> Iterator[EOVariable]:
        return self._groups.__iter__()
