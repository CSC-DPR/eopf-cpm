from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Union,
)

import xarray

from eopf.product.core.eo_mixins import EOVariableOperatorsMixin
from eopf.product.core.eo_object import EOObject

from ..formatting import renderer

if TYPE_CHECKING:
    from eopf.product.core.eo_product import EOProduct


class EOVariable(EOObject, EOVariableOperatorsMixin["EOVariable"]):
    """Wrapper around xarray.DataArray to provide Multi dimensional Array (Tensor)
    in earth observation context

    Attributes
    ----------
    name
    attrs
    dims
    coordinates
    chunksizes
    chunks
    sizes
    _data : xarray.DataArray
        inner data
    _product: EOProduct
        product top level representation to access coordinates
    """

    def __init__(
        self, name: str, data: Any, product: "EOProduct", relative_path: Optional[Iterable[str]] = None, **kwargs: Any
    ):
        if not isinstance(data, xarray.DataArray):
            data = xarray.DataArray(data=data, name=name, **kwargs)
        EOObject.__init__(self, name, product, relative_path, data.attrs)
        self._data: xarray.DataArray = data

    @property
    def dims(self) -> tuple[Hashable, ...]:
        """variable dimensions"""
        return self._data.dims

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
        self._data = self._data.chunk(chunks, name_prefix=name_prefix, token=token, lock=lock)  # pyre-ignore[6]
        return self

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
        self._data = self._data.map_blocks(func, args, kwargs, template=template)  # pyre-ignore[6]
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
                indexers=indexers,  # pyre-ignore[6]
                drop=drop,
                missing_dims=missing_dims,
                **indexers_kwargs,
            ),
            self._product,
            relative_path=self._relative_path,
        )

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
                indexers=indexers,  # pyre-ignore[6]
                method=method,  # pyre-ignore[6]
                tolerance=tolerance,
                drop=drop,
                **indexers_kwargs,
            ),
            self._product,
            relative_path=self._relative_path,
        )

    def __getitem__(self, key: Any) -> "EOVariable":
        return EOVariable(key, self._data[key], self._product, relative_path=self._relative_path)

    def __setitem__(self, key: Any, value: Any) -> None:
        self._data[key] = value

    def __delitem__(self, key: Any) -> None:
        del self._data[key]

    def __iter__(self) -> Iterator["EOVariable"]:
        for data in self._data:
            yield EOVariable(data.name, data, self._product, relative_path=self._relative_path)

    def __len__(self) -> int:
        return len(self._data)

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOVariable]{hex(id(self))}"

    def _repr_html_(self) -> str:
        return renderer("variable.html", variable=self)