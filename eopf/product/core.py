"""
Define core object compliant to the common data model
and the Climat and Forecast convention.

common data model: https://www.unidata.ucar.edu/software/netcdf/docs/netcdf_data_model.html#enhanced_model
Climate and Forecast conventions: https://cfconventions.org/
"""

from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from dataclasses import dataclass
from os import PathLike
from typing import Any, Iterable, Iterator, KeysView, List, Mapping, Optional, Union

import numpy as np
from dask.array.core import DaskMethodsMixin
from xarray import DataArray

from eopf import exceptions

from .mixins import EOVariableOperatorsMixin


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
        """print(variables)

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


class EOVariable(EOProperties, EOVariableOperatorsMixin):
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

    def __init__(self, data: Any, **kwargs: Any):
        self.__types__()
        if isinstance(data, DataArray):
            self._ndarray = data
        else:
            self._ndarray = DataArray(
                data=data,
                **kwargs,
            )

    def __str__(self):
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOVariable]{hex(id(self))}"

    def _repr_html_(self):
        return ""

    @property
    def attrs(self) -> MutableMapping[str, Any]:
        return self._ndarray.attrs

    @property
    def dims(self) -> tuple[str]:
        return self._ndarray.dims

    @property
    def dtype(self) -> np.dtype:
        return self._ndarray.dtype

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

    @property
    def chunksizes(self) -> Mapping[Any, tuple[int, ...]]:
        return self._ndarray.chunksizes

    @property
    def chunks(self) -> Optional[tuple[tuple[int, ...], ...]]:
        """Accessor to the chunks"""
        return self._ndarray.chunks

    @property
    def sizes(self) -> Mapping[str, int]:
        return self._ndarray.sizes

    def chunk(
        self,
        chunks: Union[
            Mapping[Any, Union[None, int, tuple[int, ...]]],
            int,
            tuple[int, ...],
            tuple[tuple[int, ...], ...],
        ] = {},
        name_prefix: str = "eopf-",
        token: Union[str, None] = None,
        lock: bool = False,
    ) -> None:
        """Change chunk shape / size"""
        self._ndarray = self._ndarray.chunk(chunks, name_prefix=name_prefix, token=token, lock=lock)
        return self

    def map_chunk(self, func, *args, template=None, **kwargs):
        """map function on each chunk"""
        self._ndarray = self._ndarray.map_blocks(func, args, kwargs, template)
        return self

    def compute(self, **kwargs):
        return EOVariable(self._ndarray.compute(**kwargs))

    def persiste(self, **kwargs):
        return EOVariable(self._ndarray.persiste(**kwargs))

    def __array_wrap__(self, obj, context=None) -> "EOVariable":
        self._ndarray = self._ndarray.__array_wrap__(obj, context=context)
        return self

    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        """Must be compliant to the NEP 13.
        https://numpy.org/neps/nep-0013-ufunc-overrides.html
        """
        return self._ndarray.__array_ufunc__(ufunc, method, *inputs, **kwargs)

    def __array_function__(self, func, types, args, kwargs):
        """Must be compliant to th NEP 18.
        https://numpy.org/neps/nep-0018-array-function-protocol.html
        """
        # TODO: implement to be compliant with the NEP 18
        raise NotImplementedError()

    def __dask_tokenize__(self):
        from dask.base import normalize_token

        return normalize_token((type(self), self._ndarray))

    def __dask_graph__(self):
        return self._ndarray.__dask_graph__()

    def __dask_keys__(self):
        return self._ndarray.__dask_keys__()

    def __dask_layers__(self):
        return self._ndarray.__dask_layers__()

    @property
    def __dask_optimize__(self):
        return self._ndarray.__dask_optimize__

    @property
    def __dask_scheduler__(self):
        return self._ndarray.__dask_scheduler__

    def __dask_postcompute__(self):
        finalize, extra_args = self._ndarray.__dask_postcompute__()

        def finalize_wrapper(results, name, func, *args, **kwargs):
            return EOVariable(finalize(results, name, func, *args, **kwargs))

        return finalize_wrapper, extra_args

    def __dask_postpersist__(self):
        finalize, extra_args = self._ndarray.__dask_postpersist__()

        def finalize_wrapper(results, name, func, *args, **kwargs):
            return EOVariable(finalize(results, name, func, *args, **kwargs))

        return finalize_wrapper, extra_args


class EOGroup(EOProperties, MutableMapping[str, EOVariable], DaskMethodsMixin):
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
        return self._variables[key]

    def __setitem__(self, key: str, value: EOVariable) -> None:
        self._variables[key] = value
        value.parent = self

    def __delitem__(self, key: str) -> None:
        del self._variables[key]

    def __len__(self) -> int:
        return len(self._variables)

    def __iter__(self) -> Iterator[EOVariable]:
        return iter(self._variables.values())

    def __str__(self):
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOGroup]{hex(id(self))}"

    def _repr_html_(self):
        return ""

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

    def __dask_tokenize__(self):
        from dask.base import normalize_token

        return normalize_token((type(self), self._variables, self._groups, self._attrs))

    def __dask_graph__(self):
        v_graphs = (v.__dask_graph__() for v in self._variables.values())
        variables = [v for v in v_graphs if v is not None]

        g_graph = (g.__dask_graph__() for g in self._groups.values())
        groups = [g for g in g_graph if g is not None]

        coords = self._coords.__dask_graph__() if self._coords else None

        if coords is not None:
            groups.append(coords)

        if not (variables or groups):
            return None

        from dask.highlevelgraph import HighLevelGraph

        return HighLevelGraph.merge(*variables, *groups)

    def __dask_keys__(self):
        keys = [v.__dask_keys__() for v in self._variables.values()]
        keys += [g.__dask_keys__() for g in self._groups.values()]
        if self.coords:
            keys.append(self.coords.__dask_keys__())
        return keys

    def __dask_layers__(self):
        return sum(
            self.__dask_keys__(),
            (),
        )

    def __dask_postcompute__(self):
        return self._dask_postcompute, ()

    def __dask_postpersist__(self):
        return self._dask_postpersist, ()

    def _dask_postcompute(self, results: Iterable[EOVariable]) -> "EOGroup":
        variables = []

        for v, result in zip(self._variables.values(), results):
            rebuild, args = v.__dask_postcompute__()
            variables.append(rebuild(result, *args))

        return EOGroup(
            self.name,
            *variables,
            # coords=coords,
            # groups=groups,
            dims=self._dims,
            attrs=self._attrs,
        )

    def _dask_postpersist(self, dsk: Mapping, *, rename: Mapping[str, str] = None) -> "EOGroup":
        from dask.highlevelgraph import HighLevelGraph
        from dask.optimization import cull

        variables = []

        for v in self._variables.values():
            if isinstance(dsk, HighLevelGraph):
                # dask >= 2021.3
                # __dask_postpersist__() was called by dask.highlevelgraph.
                # Don't use dsk.cull(), as we need to prevent partial layers:
                # https://github.com/dask/dask/issues/7137
                layers = v.__dask_layers__()
                if rename:
                    layers = [rename.get(k, k) for k in layers]
                dsk2 = dsk.cull_layers(layers)
            elif rename:  # pragma: nocover
                # At the moment of writing, this is only for forward compatibility.
                # replace_name_in_key requires dask >= 2021.3.
                from dask.base import flatten, replace_name_in_key

                keys = [replace_name_in_key(k, rename) for k in flatten(v.__dask_keys__())]
                dsk2, _ = cull(dsk, keys)
            else:
                # __dask_postpersist__() was called by dask.optimize or dask.persist
                dsk2, _ = cull(dsk, v.__dask_keys__())

            rebuild, args = v.__dask_postpersist__()
            # rename was added in dask 2021.3
            kwargs = {"rename": rename} if rename else {}
            variables.append(rebuild(dsk2, *args, **kwargs))

        return EOGroup(
            self.name,
            *variables,
            # coords=coords,
            # groups=groups,
            attrs=self._attrs,
            dims=self.dims,
        )

    def _ipython_key_completions_(self):
        return self.keys()


@dataclass
class MetaData:
    path: Union[str, PathLike]


class EOProduct(EOProperties, MutableMapping[str, EOGroup], DaskMethodsMixin):
    """Earth Observation Top Group definition
    Compliant to the Common data model.
    """

    __slots__ = ("_name", "_groups", "_coords", "_attrs", "_metadata")

    def __types__(self):
        self._name: str
        self._groups: MutableMapping[str, EOGroup]
        self._coords: EOGroup
        self._attrs: MutableMapping[str, Any]
        self._metadatas: Optional[Iterable[MetaData]] = None

    def __init__(
        self,
        name: str,
        coords: EOGroup,
        *args: EOGroup,
        groups: Optional[Iterable[EOGroup]] = None,
        attrs: Optional[MutableMapping[str, Any]] = None,
        metadatas: Optional[Iterable[MetaData]] = None,
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

        self._metadatas = tuple(metadata for metadata in (metadatas or []))

    def __getitem__(self, key: str) -> EOGroup:
        return self._groups[key]

    def __setitem__(self, key: str, value: EOGroup) -> None:
        self._groups[key] = value
        value.parent = self

    def __delitem__(self, key: str) -> None:
        del self._groups[key]

    def __len__(self) -> int:
        return len(self._groups)

    def __iter__(self) -> Iterator[EOGroup]:
        return iter(self._groups.values())

    def __str__(self):
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOProduct]{hex(id(self))}"

    def _repr_html_(self):
        return ""

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
    def metadatas(self) -> Optional[MetaData]:
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

    def __dask_tokenize__(self):
        from dask.base import normalize_token

        return normalize_token((type(self), self._groups, self._coords.name, self._attrs))

    def __dask_graph__(self):
        graphs = ((k, v.__dask_graph__()) for k, v in self._groups.items())
        graphs = [v for v in graphs if v is not None]
        coords_graph = self.coords.__dask_graph__()
        if coords_graph:
            graphs.append(coords_graph)
        if not graphs:
            return None

        from dask.highlevelgraph import HighLevelGraph

        return HighLevelGraph.merge(*graphs)

    def __dask_keys__(self):
        keys = [g.__dask_keys__() for g in self._groups.values()]
        keys.append(self.coords.__dask_keys__())
        return keys

    def __dask_layers__(self):
        return sum(
            self.__dask_keys__(),
            (),
        )

    @property
    def __dask_optimize__(self):
        import dask.array as da

        return da.Array.__dask_optimize__

    @property
    def __dask_scheduler__(self):
        import dask.array as da

        return da.Array.__dask_scheduler__

    def __dask_postcompute__(self):
        return self._dask_postcompute, ()

    def __dask_postpersist__(self):
        return self._dask_postpersist, ()

    def _dask_postcompute(self, results: Iterable[EOGroup]) -> "EOProduct":
        groups = []

        for v, result in zip(self._groups.values(), results):
            rebuild, args = v.__dask_postcompute__()
            groups.append(rebuild(result, *args))

        return EOGroup(
            self.name,
            *groups,
            # coords=coords,
            attrs=self._attrs,
        )

    def _dask_postpersist(self, dsk: Mapping, *, rename: Mapping[str, str] = None) -> "EOGroup":
        from dask.highlevelgraph import HighLevelGraph
        from dask.optimization import cull

        groups = []

        for v in self._groups.values():
            if isinstance(dsk, HighLevelGraph):
                # dask >= 2021.3
                # __dask_postpersist__() was called by dask.highlevelgraph.
                # Don't use dsk.cull(), as we need to prevent partial layers:
                # https://github.com/dask/dask/issues/7137
                layers = v.__dask_layers__()
                if rename:
                    layers = [rename.get(k, k) for k in layers]
                dsk2 = dsk.cull_layers(layers)
            elif rename:  # pragma: nocover
                # At the moment of writing, this is only for forward compatibility.
                # replace_name_in_key requires dask >= 2021.3.
                from dask.base import flatten, replace_name_in_key

                keys = [replace_name_in_key(k, rename) for k in flatten(v.__dask_keys__())]
                dsk2, _ = cull(dsk, keys)
            else:
                # __dask_postpersist__() was called by dask.optimize or dask.persist
                dsk2, _ = cull(dsk, v.__dask_keys__())

            rebuild, args = v.__dask_postpersist__()
            # rename was added in dask 2021.3
            kwargs = {"rename": rename} if rename else {}
            groups.append(rebuild(dsk2, *args, **kwargs))

        return EOGroup(
            self.name,
            *groups,
            # coords=coords,
            attrs=self._attrs,
        )

    def _ipython_key_completions_(self):
        return self.keys()
