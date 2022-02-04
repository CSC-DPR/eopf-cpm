import weakref
from collections.abc import MutableMapping
from types import TracebackType
from typing import (
    Any,
    Callable,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Type,
    Union,
)

import xarray

from eopf.exceptions import InvalidProductError, StoreNotDefinedError
from eopf.product.utils import join_path

from .formatting import renderer
from .mixins import EOVariableOperatorsMixin
from .store.abstract import EOProductStore, StorageStatus


class EOVariable(EOVariableOperatorsMixin["EOVariable"]):
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
        self._data: xarray.DataArray
        self._name: str = name
        self._product: EOProduct = weakref.proxy(product) if not isinstance(product, weakref.ProxyType) else product

        if relative_path is None:
            relative_path = tuple()
        self._relative_path: tuple[str, ...] = tuple(relative_path)

        if isinstance(data, xarray.DataArray):
            self._data = data
        else:
            self._data = xarray.DataArray(data=data, name=name, **kwargs)

    @property
    def _path(self) -> str:
        """path from the top level product to this eovariable"""
        if self._store is None:
            raise StoreNotDefinedError("Store must be defined")
        return join_path(*self._relative_path, self._name, sep=self._store.sep)

    @property
    def _store(self) -> Optional[EOProductStore]:
        """direct accessor to the product store"""
        return self._product._store

    @property
    def name(self) -> str:
        """name of this variable"""
        return self._name

    @property
    def attrs(self) -> dict[Hashable, Any]:
        """variable attributes"""
        return self._data.attrs

    @property
    def dims(self) -> tuple[Hashable, ...]:
        """variable dimensions"""
        return self._data.dims

    @property
    def coordinates(self) -> "EOGroup":
        """Coordinates of this variable"""
        coord = self._product.coordinates[self._path]
        if not isinstance(coord, EOGroup):
            raise TypeError(f"EOVariable coordinates type must be EOGroup instead of {type(coord)}.")
        return coord

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

    def plot(self, **kwargs: dict[Any, Any]) -> None:
        """Wrapper around the xarray plotting functionality.
        Parameters
        ----------
        The parameters MUST follow the xarray.DataArray.plot() options.
        See Also
        --------
        DataArray.plot
        """
        self._data.plot(**kwargs)

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
        return f'{self._product!r} -> {"->".join(self._relative_path)} -> {self.name}'

    def _repr_html_(self) -> str:
        return renderer("variable.html", variable=self)


class EOGroup(MutableMapping[str, Union[EOVariable, "EOGroup"]]):
    """"""

    def __init__(
        self,
        name: str,
        product: "EOProduct",
        relative_path: Optional[Iterable[str]] = None,
        dataset: Optional[xarray.Dataset] = None,
        attrs: Optional[dict[str, Any]] = None,
    ) -> None:
        self._name = name

        if relative_path is None:
            relative_path = tuple()

        if dataset is None:
            dataset = xarray.Dataset()
        else:
            for key in dataset:
                if not isinstance(key, str):
                    raise TypeError(f"The dataset key {str(key)} is type {type(key)} instead of str")
            # It is supposed by EOGroup consumer that it's dataset only contain string keys.
        if not isinstance(dataset, xarray.Dataset):
            raise TypeError("dataset parameters must be a xarray.Dataset instance")

        self._dataset = dataset
        self._relative_path: tuple[str, ...] = tuple(relative_path)
        self._product: EOProduct = weakref.proxy(product) if not isinstance(product, weakref.ProxyType) else product
        self._items: dict[str, "EOGroup"] = {}
        self._attrs = attrs or dict()

    def __getitem__(self, key: str) -> Union[EOVariable, "EOGroup"]:
        return self._get_item(key)

    def _get_item(self, key: str) -> Union[EOVariable, "EOGroup"]:
        """find and return eovariable or eogroup from the given key.

        if store is defined and key not already loaded in this group,
        data is loaded from it.
        Parameters
        ----------
        key: str
            name of the eovariable or eogroup
        """
        if key in self._dataset:
            return EOVariable(key, self._dataset[key], self._product, relative_path=[*self._relative_path, self._name])

        subkey = None
        if "/" in key:
            key, _, subkey = key.partition("/")

        item: EOGroup
        if key not in self._items and self._store is None:
            raise KeyError(f"Invalide EOGroup item name {key}")
        elif key not in self._items and self._store is not None:
            name, relative_path, dataset, attrs = self._store[self._relative_key(key)]
            item = EOGroup(name, self._product, relative_path=relative_path, dataset=dataset, attrs=attrs)
        else:
            item = self._items[key]
        self[key] = item
        if subkey is not None:
            return item[subkey]
        return item

    def __setitem__(self, key: str, value: Union[EOVariable, "EOGroup"]) -> None:
        if isinstance(value, EOGroup):
            self._items[key] = value
        elif isinstance(value, EOVariable):
            self._dataset[value.name] = value
        else:
            raise TypeError(f"Item assigment Impossible for type {type(value)}")

    def __delitem__(self, key: str) -> None:
        if key in self._items:
            del self._items[key]
        if key in self._dataset:
            del self._dataset[key]
        if self._store is not None and (store_key := self._relative_key(key)) in self._store:
            del self._store[store_key]

    def __iter__(self) -> Iterator[str]:
        if self._store is not None:
            for key in self._store.iter(self._path):  # pyre-ignore[16]
                if key not in self._items and key not in self._dataset:
                    yield key
        if self._dataset is not None:
            yield from self._dataset  # type: ignore[misc]
        yield from self._items

    def __len__(self) -> int:
        keys = set(self._items)
        if self._store is not None:
            keys |= set(self._store[self._path])
        return len(keys)

    def __getattr__(self, attr: str) -> Union[EOVariable, "EOGroup"]:
        if attr in self:
            return self[attr]
        raise AttributeError(attr)

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOGroup]{hex(id(self))}"

    def _repr_html_(self) -> str:
        return renderer("group.html", group=self)

    def to_product(self) -> "EOProduct":
        """Convert this group to a product"""
        ...

    @property
    def coordinates(self) -> "EOGroup":
        """Coordinates of this group"""
        coord = self._product.coordinates[self._path]
        if not isinstance(coord, EOGroup):
            raise TypeError(f"EOGroup coordinates type must be EOGroup instead of {type(coord)}.")
        return coord

    @property
    def _store(self) -> Optional[EOProductStore]:
        """direct accessor to the product store"""
        return self._product._store

    @property
    def _path(self) -> str:
        """path from the top level product to this eogroup"""
        if self._store is None:
            raise StoreNotDefinedError("Store must be defined")
        return join_path(*self._relative_path, self._name, sep=self._store.sep)

    @property
    def name(self) -> str:
        """name of this eogroup"""
        return self._name

    @property
    def attrs(self) -> dict[str, Any]:
        return self._attrs

    @property
    def dims(self) -> tuple[Hashable, ...]:
        """dimension of this group"""
        return tuple(self._dataset.dims)

    @property
    def groups(self) -> Iterator[tuple[str, "EOGroup"]]:
        """iterator over the groups of this group"""
        for key, value in self.items():
            if isinstance(value, EOGroup):
                yield key, value

    @property
    def variables(self) -> Iterator[tuple[str, EOVariable]]:
        """iterator over the variables of this group"""
        for key, value in self.items():
            if isinstance(value, EOVariable):
                yield key, value

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
        if self._store is None:
            raise StoreNotDefinedError("Store must be defined")
        return join_path(*self._relative_path, self._name, key, sep=self._store.sep)

    def add_group(self, name: str) -> "EOGroup":
        """Construct and add a eogroup to this group

        if store is defined and open, the group it's directly write by the store.
        Parameters
        ----------
        name: str
            name of the future group
        Returns
        -------
        EOGroup
            newly created EOGroup
        """
        relative_path = [*self._relative_path, self.name]
        keys = None
        if "/" in name:
            name, _, keys = name.partition("/")
        group = EOGroup(name, self._product, relative_path=relative_path)
        self[name] = group
        if self._store is not None and self._store.status == StorageStatus.OPEN:
            self._store.add_group(name, relative_path=relative_path)

        if keys is not None:
            group = self[name].add_group(keys)  # type:ignore[union-attr]
            # We juste created it, type shouldn't have changed.
        return group

    def add_variable(self, name: str, data: Optional[Any] = None, **kwargs: Any) -> EOVariable:
        """Construct and add an eovariable to this group

        if store is defined and open, the eovariable it's directly write by the store.
        Parameters
        ----------
        name: str
            name of the future group
        data: any, optional
            data like object use to create dataarray or dataarray
        **kwargs: any
            DataArray extra parameters if data is not a dataarray
        Returns
        -------
        EOVariable
            newly created EOVariable
        """
        variable = EOVariable(name, data, self._product, relative_path=[*self._relative_path, self._name], **kwargs)
        self._dataset[name] = variable._data
        if self._store is not None and self._store.status == StorageStatus.OPEN:
            self._store.add_variables(self._name, self._dataset, relative_path=self._relative_path)
        return variable

    def write(self) -> None:
        """
        write non synchronized subgroups, variables to the store

        the store must be opened to work
        See Also
        --------
        EOProduct.open
        """
        if self._store is None:
            raise StoreNotDefinedError("Store must be defined")
        for name, item in self.groups:
            if name not in self._store.iter(self._path):  # pyre-ignore[16]
                self._store.add_group(name, relative_path=[*self._relative_path, self._name])  # pyre-ignore[16]
            item.write()
        if self._dataset is not None and len(self._dataset) > 0:
            self._store.add_variables(self._name, self._dataset, relative_path=self._relative_path)  # pyre-ignore[16]

    def _ipython_key_completions_(self) -> list[str]:
        return [key for key in self.keys()]

    def __contains__(self, key: object) -> bool:
        return (
            (key in self._items)
            or (key in self._dataset)
            or (self._store is not None and key in self._store.iter(self._path))  # type: ignore[operator]
        )


class EOProduct(MutableMapping[str, Union[EOVariable, "EOGroup"]]):
    """"""

    MANDATORY_FIELD = ("measurements", "coordinates", "attributes")

    def __init__(self, name: str, store_or_path_url: Optional[Union[str, EOProductStore]] = None) -> None:
        self._name: str = name
        self._groups: dict[str, EOGroup] = {}
        self._store: Optional[EOProductStore] = None
        self.__set_store(store_or_path_url=store_or_path_url)

    def __set_store(self, store_or_path_url: Optional[Union[str, EOProductStore]] = None) -> None:
        from .store.zarr import EOZarrStore

        if isinstance(store_or_path_url, str):
            self._store = EOZarrStore(store_or_path_url)
        elif isinstance(store_or_path_url, EOProductStore):
            self._store = store_or_path_url
        elif store_or_path_url is not None:
            raise TypeError(f"{type(store_or_path_url)} can't be used to instantiate EOProductStore.")

    def __getitem__(self, key: str) -> Union[EOVariable, "EOGroup"]:
        return self._get_group(key)

    def __setitem__(self, key: str, value: Union[EOVariable, "EOGroup"]) -> None:
        if not isinstance(value, EOGroup):
            raise NotImplementedError
        self._groups[key] = value

    def __iter__(self) -> Iterator[str]:
        if self._store is not None:
            for key in self._store:  # pyre-ignore[16]
                if key not in self._groups:
                    yield key
        yield from self._groups

    def __delitem__(self, key: str) -> None:
        if key in self._groups:
            del self._groups[key]
        if self._store and key in self._store:
            del self._store[key]

    def __len__(self) -> int:
        keys = set(self._groups)
        if self._store is not None:
            keys |= set(self._store)
        return len(keys)

    def __getattr__(self, attr: str) -> Union[EOVariable, "EOGroup"]:
        if attr in self:
            return self[attr]
        raise AttributeError(attr)

    def __contains__(self, key: object) -> bool:
        return (key in self._groups) or (self._store is not None and key in self._store)

    def _get_group(self, group_name: str) -> Union[EOVariable, "EOGroup"]:
        """find and return eogroup from the given key.

        if store is defined and key not already loaded in this group,
        data is loaded from it.
        Parameters
        ----------
        key: str
            name of the eogroup
        """

        subgroup_name = None
        if "/" in group_name:
            group_name, _, subgroup_name = group_name.partition("/")

        group = self._groups.get(group_name)
        if group is None:
            if self._store is None:
                raise KeyError(f"Invalide EOGroup name: {group_name}")
            name, relative_path, dataset, attrs = self._store[group_name]
            group = EOGroup(name, self, relative_path=relative_path, dataset=dataset, attrs=attrs)
            self[group_name] = group

        if subgroup_name is not None:
            return group[subgroup_name]
        return group

    def add_group(self, name: str) -> EOGroup:
        """Construct and add a eogroup to this product

        if store is defined and open, the group it's directly write by the store.
        Parameters
        ----------
        name: str
            name of the future group
        Returns
        -------
        EOGroup
            newly created EOGroup
        """
        keys = None
        if "/" in name:
            name, _, keys = name.partition("/")
        group = EOGroup(name, self, relative_path=[])
        self[name] = group
        if self._store is not None and self._store.status == StorageStatus.OPEN:
            self._store.add_group(name)
        if keys is not None:
            group = self[name].add_group(keys)  # type:ignore[union-attr]
        return group

    @property
    def name(self) -> str:
        """name of the product"""
        return self._name

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOProduct]{hex(id(self))}"

    def _repr_html_(self) -> str:
        return renderer("product.html", product=self)

    def _ipython_key_completions_(self) -> list[str]:
        return [key for key in self.keys()]

    def open(
        self, *, store_or_path_url: Optional[Union[EOProductStore, str]] = None, mode: str = "r", **kwargs: Any
    ) -> "EOProduct":
        """setup the store to be readable or writable

        if store_or_path_url is given, the store is overwrite by the new one.

        Parameters
        ----------
        store_or_path_url: EOProductStore or str, optional
            the new store or a path url the target file system
        mode: str, optional
            mode to open the store
        **kwargs: Any
            extra kwargs to open the store
        """
        if store_or_path_url:
            self.__set_store(store_or_path_url=store_or_path_url)
        if self._store is None:
            raise StoreNotDefinedError("Store must be defined")
        self._store.open(mode=mode, **kwargs)
        return self

    def load(self) -> None:
        """load all the product in memory"""
        if self._store is None:
            raise StoreNotDefinedError("Store must be defined")
        for key in self._store:
            if key not in self._groups:
                ...

    def write(self) -> None:
        """
        write non synchronized subgroups, variables to the store

        the store must be opened to work
        See Also
        --------
        EOGroup.open
        """
        if self._store is None:
            raise StoreNotDefinedError("Store must be defined")
        self.validate()
        for name, group in self._groups.items():
            if name not in self._store:  # pyre-ignore[58]
                self._store.add_group(name)  # pyre-ignore[16]
            group.write()

    def is_valid(self) -> bool:
        """check if the product is a valid eopf product
        See Also
        --------
        EOProduct.validate"""
        return all(key in self for key in self.MANDATORY_FIELD)

    def validate(self) -> None:
        """check if the product is a valid eopf product, raise an error if is not a valid one

        See Also
        --------
        EOProduct.is_valid
        """
        if not self.is_valid():
            raise InvalidProductError(f"Invalid product {self}, missing mandatory groups.")

    def __enter__(self) -> "EOProduct":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if self._store is None:
            raise StoreNotDefinedError("Store must be defined")
        self._store.close()

    def _create_structure(self, group: Union[EOGroup, tuple[str, EOGroup]], level: int) -> None:
        if isinstance(group, tuple):
            group = group[1]
        for v in group.variables:
            print("|" + " " * level + "└──", v[0])
        for g in group.groups:
            print("|" + " " * level + "├──", g[0])
            self._create_structure(g, level + 2)

    def tree(self) -> Union["EOProduct", None]:
        """Display the hierarchy of the product.

        Returns
        ------
        Instance of EOProduct if the environment is interactive (e.g. Jupyter Notebook)
        Oterwise, returns None.
        """
        try:
            from IPython import get_ipython

            if get_ipython():
                return self
            for name, group in self._groups.items():
                print(f"├── {name}")
                self._create_structure(group, level=2)
        except ModuleNotFoundError:
            print("IPython cannot be found.")
        return None
