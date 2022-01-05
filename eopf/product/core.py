"""
Define core object compliant to the common data model
and the Climat and Forecast convention.

common data model: https://www.unidata.ucar.edu/software/netcdf/docs/netcdf_data_model.html#enhanced_model
Climate and Forecast conventions: https://cfconventions.org/
"""

import itertools as it
from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from dataclasses import dataclass
from os import PathLike
from typing import (
    Any,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    Optional,
    Union,
    ValuesView,
)

import numpy as np
from dask.array.core import DaskMethodsMixin
from xarray import DataArray

from eopf import exceptions
from eopf.product.formatting import renderer

from .mixins import EOVariableOperatorsMixin


def search(eoobject: Union["EOGroup", "EOProduct"], path, already_done: Optional[list["EOGroup"]] = None):
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
    else:  # proximityTuple of dimension names associated with this array.
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
        """str: Name of this object"""

    @property
    @abstractmethod
    def dims(self) -> tuple[str, ...]:
        """
        tuple[str, ...]: Tuple of dimension names associated with this object.
        """

    @property
    @abstractmethod
    def attrs(self) -> MutableMapping[str, Any]:
        """
        MutableMapping[str, Any]: Dictionary storing arbitrary metadata with this object.
        """

    @property
    @abstractmethod
    def parent(self) -> Union["EOProduct", "EOGroup", None]:
        """
        Union[EOProduct, EOGroup]: Direct parent object
        """


class EOVariable(EOProperties, EOVariableOperatorsMixin):
    """Earth Observation Variable

    EOVariable is a N-dimensional array (Tensor) with indexation and selection
    capabilities and compliant with most of the python numerical object like:
    - numpy array
    - pandas object

    Parameters
    ----------
        data: array_like
            xarray.DataArray or array_like.
        **kwargs: Any
            if data is not a xarray.DataArray, you can provide
            xarray.DataArray arguments to passing them here.

    Examples
    --------
    Create EOVariable:

    >>> data = np.random.normal(size=(10, 10))
    >>> variable = EOVariable(data, name="normal_distribution")
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

    def __getitem__(self, key: Any) -> DataArray:
        return EOVariable(self._ndarray[key])

    def __setitem__(self, key: Any, value: Any) -> None:
        self._ndarray[key] = value

    def __delitem__(self, key: Any) -> None:
        del self._ndarray[key]

    def __str__(self):
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOVariable]{hex(id(self))}"

    def _repr_html_(self):
        return renderer("variable.html", variable=self)

    @property
    def attrs(self) -> MutableMapping[str, Any]:
        """
        MutableMapping[str, Any]: Dictionary storing arbitrary metadata with this object.
        """
        return self._ndarray.attrs

    @property
    def dims(self) -> tuple[str]:
        """
        tuple[str, ...]: Tuple of dimension names associated with this object.
        """
        return self._ndarray.dims

    @property
    def dtype(self) -> np.dtype:
        return self._ndarray.dtype

    @property
    def name(self) -> Optional[str]:
        """str: Name of this object"""
        return self._ndarray.name

    @property
    def parent(self) -> "EOGroup":
        """
        Union[EOProduct, EOGroup]: Direct parent object
        """
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
        """
        Mapping from dimension names to block lengths for this dataarray's data, or None if
        the underlying data is not a dask array.
        Cannot be modified directly, but can be modified by calling .chunk().

        Differs from EOVariable.chunks because it returns a mapping of dimensions to chunk shapes
        instead of a tuple of chunk shapes.

        See Also
        --------
        EOVariable.chunk
        EOVariable.chunks
        """
        return self._ndarray.chunksizes

    @property
    def chunks(self) -> Optional[tuple[tuple[int, ...], ...]]:
        """
        Tuple of block lengths for this dataarray's data, in order of dimensions, or None if
        the underlying data is not a dask array.

        See Also
        --------
        EOVariable.chunk
        EOVariable.chunksizes
        """
        return self._ndarray.chunks

    @property
    def sizes(self) -> Mapping[str, int]:
        """
        Ordered mapping from dimension names to lengths.

        Immutable.
        """
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
    ) -> "EOVariable":
        """Coerce this array's data into a dask arrays with the given chunks.

        If this variable is a non-dask array, it will be converted to dask
        array. If it's a dask array, it will be rechunked to the given chunk
        sizes.

        If neither chunks is not provided for one or more dimensions, chunk
        sizes along that dimension will not be updated; non-dask arrays will be
        converted into dask arrays with a single block.

        Parameters
        ----------
        chunks : int, tuple of int or mapping of hashable to int, optional
            Chunk sizes along each dimension, e.g., ``5``, ``(5, 5)`` or
            ``{'x': 5, 'y': 5}``.
        name_prefix : str, optional
            Prefix for the name of the new dask array.
        token : str, optional
            Token uniquely identifying this array.
        lock : optional
            Passed on to :py:func:`dask.array.from_array`, if the array is not
            already as dask array.

        Returns
        -------
        chunked : eopf.product.EOVariable
        """
        self._ndarray = self._ndarray.chunk(chunks, name_prefix=name_prefix, token=token, lock=lock)
        return self

    def map_chunk(self, func, *args, template=None, **kwargs):
        """
        Apply a function to each chunk of this EOVariable.

        .. warning::
            This method is based on the experimental method ``DataArray.map_blocks`` and its signature may change.

        Parameters
        ----------
        func : callable
            User-provided function that accepts a DataArray as its first
            parameter. The function will receive a subset or 'block' of this EOVariable (see below),
            corresponding to one chunk along each chunked dimension. ``func`` will be
            executed as ``func(subset_dataarray, *subset_args, **kwargs)``.

            This function must return either a single EOVariable.

            This function cannot add a new chunked dimension.
        args : sequence
            Passed to func after unpacking and subsetting any eovariable objects by blocks.
            eovariable objects in args must be aligned with this object, otherwise an error is raised.
        kwargs : mapping
            Passed verbatim to func after unpacking. eovariable objects, if any, will not be
            subset to blocks. Passing dask collections in kwargs is not allowed.
        template : DataArray or Dataset, optional
            eovariable object representing the final result after compute is called. If not provided,
            the function will be first run on mocked-up data, that looks like this object but
            has sizes 0, to determine properties of the returned object such as dtype,
            variable names, attributes, new dimensions and new indexes (if any).
            ``template`` must be provided if the function changes the size of existing dimensions.
            When provided, ``attrs`` on variables in `template` are copied over to the result. Any
            ``attrs`` set by ``func`` will be ignored.

        Returns
        -------
        A single DataArray or Dataset with dask backend, reassembled from the outputs of the
        function.

        See Also
        --------
        dask.array.map_blocks, xarray.apply_ufunc, xarray.Dataset.map_blocks, xarray.DataArray.map_blocks
        """
        self._ndarray = self._ndarray.map_blocks(func, args, kwargs, template)
        return self

    def compute(self, **kwargs):
        """Manually trigger loading of this array's data from disk or a
        remote source into memory and return a new array. The original is
        left unaltered.

        Normally, it should not be necessary to call this method in user code,
        because all xarray functions should either work on deferred data or
        load data automatically. However, this method can be necessary when
        working with many file objects on disk.

        Parameters
        ----------
        **kwargs : dict
            Additional keyword arguments passed on to ``dask.compute``.

        See Also
        --------
        xarray.DataArray.compute
        dask.compute
        """
        return EOVariable(self._ndarray.compute(**kwargs))

    def persiste(self, **kwargs):
        """Trigger computation in constituent dask arrays

        This keeps them as dask arrays but encourages them to keep data in
        memory.  This is particularly useful when on a distributed machine.
        When on a single machine consider using ``.compute()`` instead.

        Parameters
        ----------
        **kwargs : dict
            Additional keyword arguments passed on to ``dask.persist``.

        See Also
        --------
        xarray.DataArray.persist
        dask.persist
        """
        return EOVariable(self._ndarray.persiste(**kwargs))

    def isel(
        self,
        indexers: Mapping[Any, Any] = None,
        drop: bool = False,
        missing_dims: str = "raise",
        **indexers_kwargs: Any,
    ):
        """Return a new EOVariable whose data is given by integer indexing
        along the specified dimension(s).

        Parameters
        ----------
        indexers : dict, optional
            A dict with keys matching dimensions and values given
            by integers, slice objects or arrays.
            indexer can be a integer, slice, array-like or EOVariable.
            If EOVariables are passed as indexers, xarray-style indexing will be
            carried out.
            One of indexers or indexers_kwargs must be provided.
        drop : bool, optional
            If ``drop=True``, drop coordinates variables indexed by integers
            instead of making them scalar.
        missing_dims : {"raise", "warn", "ignore"}, default: "raise"
            What to do if dimensions that should be selected from are not present in the
            EOVariable:
            - "raise": raise an exception
            - "warn": raise a warning, and ignore the missing dimensions
            - "ignore": ignore the missing dimensions
        **indexers_kwargs : {dim: indexer, ...}, optional
            The keyword arguments form of ``indexers``.

        See Also
        --------
        DataArray.sel
        DataArray.isel
        EOVariable.sel
        """
        return EOVariable(
            self._ndarray.isel(
                indexers=indexers,
                drop=drop,
                missing_dims=missing_dims,
                **indexers_kwargs,
            ),
        )

    def sel(
        self,
        indexers: Mapping[Any, Any] = None,
        method: str = None,
        tolerance=None,
        drop: bool = False,
        **indexers_kwargs: Any,
    ):
        """Return a new EOVariable whose data is given by selecting index
        labels along the specified dimension(s).

        In contrast to `EOVariable.isel`, indexers for this method should use
        labels instead of integers.

        Under the hood, this method is powered by using pandas's powerful Index
        objects. This makes label based indexing essentially just as fast as
        using integer indexing.

        It also means this method uses pandas's (well documented) logic for
        indexing. This means you can use string shortcuts for datetime indexes
        (e.g., '2000-01' to select all values in January 2000). It also means
        that slices are treated as inclusive of both the start and stop values,
        unlike normal Python indexing.

        .. warning::

          Do not try to assign values when using any of the indexing methods
          ``isel`` or ``sel``::

            da = xr.EOVariable([0, 1, 2, 3], dims=['x'])
            # DO NOT do this
            da.isel(x=[0, 1, 2])[1] = -1

          Assigning values with the chained indexing using ``.sel`` or
          ``.isel`` fails silently.

        Parameters
        ----------
        indexers : dict, optional
            A dict with keys matching dimensions and values given
            by scalars, slices or arrays of tick labels. For dimensions with
            multi-index, the indexer may also be a dict-like object with keys
            matching index level names.
            If EOVariables are passed as indexers, xarray-style indexing will be
            carried out.
            One of indexers or indexers_kwargs must be provided.
        method : {None, "nearest", "pad", "ffill", "backfill", "bfill"}, optional
            Method to use for inexact matches:

            * None (default): only exact matches
            * pad / ffill: propagate last valid index value forward
            * backfill / bfill: propagate next valid index value backward
            * nearest: use nearest valid index value
        tolerance : optional
            Maximum distance between original and new labels for inexact
            matches. The values of the index at the matching locations must
            satisfy the equation ``abs(index[indexer] - target) <= tolerance``.
        drop : bool, optional
            If ``drop=True``, drop coordinates variables in `indexers` instead
            of making them scalar.
        **indexers_kwargs : {dim: indexer, ...}, optional
            The keyword arguments form of ``indexers``.
            One of indexers or indexers_kwargs must be provided.

        Returns
        -------
        obj : EOVariable
            A new EOVariable with the same contents as this EOVariable, except the
            data and each dimension is indexed by the appropriate indexers.
            If indexer EOVariables have coordinates that do not conflict with
            this object, then these coordinates will be attached.
            In general, each array's data will be a view of the array's data
            in this EOVariable, unless vectorized indexing was triggered by using
            an array indexer, in which case the data will be a copy.

        See Also
        --------
        DataArray.isel
        DataArray.sel
        EOVariable.isel
        """
        return EOVariable(
            self._ndarray.sel(
                indexers=indexers,
                method=method,
                tolerance=tolerance,
                drop=drop,
                **indexers_kwargs,
            ),
        )

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
    """Earth Observation Group

    A hierarchical object used to organised Earth observation data like EOVariable and
    sub EOGroup.

    Parameters
    ----------
    name: str
        name of this group
    *args: EOVariable
        list of the EOVariables associated to this group
    variables: Iterable[EOVariable], optional
        list of the EOVariables associated to this group, like args
    groups: Iterable[EOGroup], optional
        list of the sub EOGroup
    coords: EOGroup, optional, optional
        coordinates EOGroup linked to this EOGroup
    attrs: MutableMapping[str, Any], optional
        Dictionnary like of metadatas
    dims: Iterable[str], optional
        Iterable corresponding to the name of each dimensions
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
        self._name: int
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
        dims: Optional[Iterable[str]] = None,
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
        return renderer("group.html", group=self)

    @property
    def attrs(self):
        """
        MutableMapping[str, Any]: Dictionary storing arbitrary metadata with this object.
        """
        return self._attrs

    @property
    def coords(self) -> "EOGroup":
        """
        EOGroup: coordinates representation of this eogroup
        """
        return self._coords

    @property
    def dims(self) -> tuple[str]:
        """
        tuple[str, ...]: Tuple of dimension names associated with this object.
        """
        return self._dims

    @property
    def groups(self) -> MutableMapping[str, "EOGroup"]:
        """
        MutableMapping[str, EOGroup]: sub EOGroups
        """
        return self._groups

    @property
    def name(self) -> Optional[str]:
        """str: Name of this object"""
        return self._name

    @property
    def parent(self):
        """
        Union[EOProduct, EOGroup]: Direct parent object
        """
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

    def values(self) -> ValuesView["EOGroup"]:
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
        groups = []
        coords_results = None
        coords_group = None
        groups_results: MutableMapping[str, Iterable[EOVariable]] = {}

        for result in results:
            if (name := result.name) in self._variables:
                v = self._variables.get(name)
                rebuild, args = v.__dask_postcompute__()
                variables.append(rebuild(result, *args))
            else:
                for group_name, group in self.groups.items():
                    if result.__dask_tokenize__() in group.__dask_keys__():
                        groups_results.setdefault(group_name, []).append(result)
                        break
                else:
                    if self.coords and result.__dask_tokenize__() in self.coords.__dask_keys__():
                        if coords_results is None:
                            coords_results = []
                        coords_results.append(result)

        for group_name, group_results in groups_results.items():
            group = self.groups.get(group_name)
            rebuild, args = group.__dask_postcompute__()
            groups.append(rebuild(group_results, *args))

        if coords_results:
            rebuild, args = self.coords.__dask_postcompute__()
            coords_group = rebuild(coords_results, *args)

        return EOGroup(
            self.name,
            *variables,
            coords=coords_group,
            groups=groups,
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
    """Earth Observation Product

    Represent the top group level, called Product.

    Parameters
    ----------
    name: str
        product name
    coords: EOGroup
        coordinates associated to this product
    *args: EOGroup
        sub EOGroup
    group: Iterable[EOGroup], optional
        sub EOGroups, like args
    attrs: MutableMapping[str, Any]
        Attribute key, value
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
        return renderer("product.html", product=self)

    @property
    def attrs(self) -> MutableMapping[str, Any]:
        """
        MutableMapping[str, Any]: Dictionary storing arbitrary metadata with this object.
        """
        return self._attrs

    @property
    def coords(self) -> EOGroup:
        """
        EOGroup: coordinates representation of this EOProduct
        """
        return self._coords

    @property
    def dims(self) -> tuple[str]:
        """
        tuple: Empty tuple, an EOProduct does'nt have dimensions
        """
        return tuple()

    @property
    def metadatas(self) -> Optional[MetaData]:
        return self._metadata

    @property
    def name(self) -> str:
        """str: Name of this object"""
        return self._name

    @property
    def parent(self) -> None:
        """
        As top level, EOProduct does'nt have parent
        """
        return None

    def keys(self) -> KeysView["EOProduct"]:
        return self._groups.keys()

    def values(self) -> KeysView["EOProduct"]:
        return self._groups.values()

    def __dask_tokenize__(self):
        from dask.base import normalize_token

        return normalize_token((type(self), self._groups, self._coords.name, self._attrs))

    def __dask_graph__(self):
        graphs = (v.__dask_graph__() for v in self._groups.values())
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

    def _dask_postcompute(self, results: Iterable[EOVariable]) -> "EOProduct":
        groups = []
        coords = None

        for result in it.product(results):

            if (name := result.name) in self._groups:
                g = self._groups.get(name)
                rebuild, args = g.__dask_postcompute__()
                groups.append(rebuild(result, *args))
            else:
                if (name := result.name) == self.coords.name:
                    rebuild, args = self.coords.__dask_postcompute__()
                    coords = rebuild(result, *args)

        return EOProduct(
            self.name,
            *groups,
            coords=coords,
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
