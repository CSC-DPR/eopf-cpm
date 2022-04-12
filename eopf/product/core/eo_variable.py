from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Union,
    ValuesView,
)

import xarray
from dask import array as da

from eopf.product.core.eo_mixins import EOVariableOperatorsMixin
from eopf.product.core.eo_object import EOObject

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_container import EOContainer


class EOVariable(EOObject, EOVariableOperatorsMixin["EOVariable"]):
    """Wrapper around xarray.DataArray to provide Multi dimensional Array (Tensor)
    in earth observation context

    Parameters
    ----------
    name: str, optional
        name of this group
    data: any, optional
        any data accept by :obj:`xarray.DataArray`
    parent: EOProduct or EOGroup, optional
        parent to link to this group
    attrs: MutableMapping[str, Any], optional
        attributes to assign
    dims: tuple[str], optional
        dimensions to assign
    **kwargs: Any
        any arguments to construct an :obj:`xarray.DataArray`

    See Also
    --------
    xarray.DataArray
    """

    def __init__(
        self,
        name: str = "",
        data: Optional[Any] = None,
        parent: Optional["EOContainer"] = None,
        attrs: Optional[MutableMapping[str, Any]] = None,
        dims: tuple[str, ...] = tuple(),
        **kwargs: Any,
    ):
        attrs = dict(attrs) if attrs is not None else {}

        from .eo_object import _DIMENSIONS_NAME

        existing_dims = attrs.pop(_DIMENSIONS_NAME, [])
        if not isinstance(data, (xarray.DataArray, EOVariable)) and data is not None:
            lazy_data = da.asarray(data)
            if not hasattr(data, "dtype"):
                data = xarray.DataArray(data=lazy_data, name=name, attrs=attrs, **kwargs)
            else:
                input_dtype = data.dtype
                data = xarray.DataArray(data=lazy_data, name=name, attrs=attrs, **kwargs).astype(input_dtype)
        elif isinstance(data, EOVariable):
            data = xarray.DataArray(data=data._data, attrs=data.attrs | attrs, dims=data.dims)
        elif isinstance(data, xarray.DataArray):
            data = data.copy()
            data.attrs.update(attrs)

        if data is None:
            data = xarray.DataArray(name=name, attrs=attrs, **kwargs)

        if not dims:
            dims = existing_dims or data.dims
        self._data: xarray.DataArray = data
        EOObject.__init__(self, name, parent, dims=tuple(dims))

    def _init_similar(self, data: xarray.DataArray) -> "EOVariable":
        return EOVariable(name="", data=data)

    def assign_dims(self, dims: Iterable[str]) -> None:
        dims = tuple(dims)
        if len(dims) != len(self._data.dims):
            raise ValueError("Invalid number of dimensions.")
        self._data = self._data.swap_dims(dict(zip(self._data.dims, dims)))
        super().assign_dims(dims)

    # docstr-coverage: inherited
    @property
    def attrs(self) -> dict[str, Any]:
        return self._data.attrs

    # docstr-coverage: inherited
    @property
    def coords(self) -> ValuesView[Any]:
        return self.coordinates.values()

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
        return self._data.chunksizes

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
        return self._data.chunks

    def compute(self, **kwargs: Any) -> "EOVariable":
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
        return self._init_similar(self._data.compute())

    @property
    def data(self):
        return self._data.data

    @property
    def sizes(self) -> Mapping[Hashable, int]:
        """
        Ordered mapping from dimension names to lengths.
        Immutable.
        """
        return self._data.sizes

    def chunk(
        self,
        chunks: Union[
            Mapping[Any, Optional[Union[int, tuple[int, ...]]]],
            int,
            tuple[int, ...],
            tuple[tuple[int, ...], ...],
        ] = {},
        name_prefix: str = "eopf-",
        token: Optional[str] = None,
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
        self._data = self._data.chunk(chunks, name_prefix=name_prefix, token=token, lock=lock)
        return self

    @property
    def loc(self) -> "_LocIndexer":
        return _LocIndexer(self)

    def map_chunk(
        self,
        func: Callable[..., xarray.DataArray],
        *args: Any,
        template: Optional[xarray.DataArray] = None,
        **kwargs: Any,
    ) -> "EOVariable":
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
        self._data = self._data.map_blocks(func, args, kwargs, template=template)
        return self

    def isel(
        self,
        indexers: Optional[Mapping[Any, Any]] = None,
        drop: bool = False,
        missing_dims: str = "raise",
        **indexers_kwargs: Any,
    ) -> "EOVariable":
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
            self.name,
            self._data.isel(
                indexers=indexers,
                drop=drop,
                missing_dims=missing_dims,
                **indexers_kwargs,
            ),
        )

    def persist(self, **kwargs: Any) -> "EOVariable":
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
        xarray.Dataset.persist
        dask.persist
        """
        return self._init_similar(self._data.persist())

    def sel(
        self,
        indexers: Optional[Mapping[Any, Any]] = None,
        method: Optional[str] = None,
        tolerance: Any = None,
        drop: bool = False,
        **indexers_kwargs: Any,
    ) -> "EOVariable":
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
            self.name,
            self._data.sel(
                indexers=indexers,
                method=method,
                tolerance=tolerance,
                drop=drop,
                **indexers_kwargs,
            ),
        )

    def plot(self, **kwargs: dict[Any, Any]) -> None:
        """Wrapper around the xarray plotting functionality.

        Parameters
        ----------
        The parameters MUST follow the xarray.DataArray.plot() options.

        See Also
        --------
        DataArray.plot
        """
        import warnings

        try:
            self._data.plot(**kwargs)
        except Exception as e:
            warnings.warn(f"Cannot display plot. Error {e}")

    @property
    def shape(self) -> tuple[int, ...]:
        return self._data.shape

    def __getitem__(self, key: Any) -> "EOVariable":
        data = self._data[key]
        return EOVariable(self.name, data)

    def __setitem__(self, key: Any, value: Any) -> None:
        self._data[key] = value

    def __delitem__(self, key: Any) -> None:
        del self._data[key]

    def __iter__(self) -> Iterator["EOVariable"]:
        for data in self._data:
            yield EOVariable(data.name, data)

    def __len__(self) -> int:
        return len(self._data)

    def __str__(self) -> str:
        from .eo_object import _DIMENSIONS_NAME

        if self.coordinates:
            coordinates = "\n".join(["Coordinates:", *map(lambda x: f"    {x}", self.coordinates.keys())])
        else:
            coordinates = ""
        attrs = [f"    {key}: {value}" for key, value in self.attrs.items() if key != _DIMENSIONS_NAME]
        if attrs:
            attributes = "\n".join(["Attributes:", *attrs])
        else:
            attributes = ""
        return "\n".join(
            [
                f"<eopf.product.EOVariable {self.path}>",
                "Data:",
                f"    {self._data.data}",
                *filter(lambda x: x != "", [coordinates, attributes]),
            ],
        )

    def __repr__(self) -> str:
        return str(self)

    def _repr_html_(self) -> str:
        from ..formatting import renderer

        return renderer("variable.html", variable=self)


class _LocIndexer:
    __slots__ = ("variable",)

    def __init__(self, variable: EOVariable):
        self.variable = variable

    def __getitem__(self, key: Any) -> EOVariable:
        return EOVariable(self.variable.name, self.variable._data.loc[key])

    def __setitem__(self, key: Any, value: Any) -> None:
        self.variable._data[key] = value
