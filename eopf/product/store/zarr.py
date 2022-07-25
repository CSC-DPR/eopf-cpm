import pathlib
from typing import TYPE_CHECKING, Any, Iterator, MutableMapping, Optional

import dask
import zarr
from dask import array as da
from dask.delayed import Delayed
from numcodecs import Blosc
from zarr.hierarchy import Group
from zarr.storage import FSStore, contains_array, contains_group

from eopf.exceptions import StoreNotOpenError
from eopf.product.utils import conv

from .abstract import EOProductStore

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


class EOZarrStore(EOProductStore):
    """Store representation to access to a Zarr file on the given URL.

    Url can be used to access to S3 and/or zip files.

    .. warning::
        zip file on S3 is not available in writing mode.

    Parameters
    ----------
    url: str
        path url or the target store

    Attributes
    ----------
    url: str
        url to the target store

    Examples
    --------
    >>> zarr_store = EOZarrStore("zip::s3://eopf_storage/S3A_OL_1_EFR___LN1_O_NT_002.zarr")
    >>> product = EOProduct("OLCI Product", storage=zarr_store)
    >>> with open_store(product, storage_options=storage_options):
    ...    measurements = product["/measurements"]

    Notes
    -----
    URL can be one of the following format:

        * <path_to_my_file>
        * s3://<path_to_my_file>
            ``zarr.open(mode=mode, storage_options=storage_options)``
        * zip::s3://<path_to_my_file>
            ``zarr.open(mode=mode, storage_options=storage_options)``

    Storage options is used to identify s3 storage:
        >>> storage_options = dict(
        ...     key="access_key",
        ...     secret="secret_key",
        ...     client_kwargs={
        ...         "endpoint_url": "my_endpoint",
        ...         "region_name": "region_name"
        ...     }
        ... )

    if you read a zip on s3 storage:
        >>> storage_options = dict(s3=dict(
        ...     key="access_key",
        ...     secret="secret_key",
        ...     client_kwargs={
        ...         "endpoint_url": "my_endpoint",
        ...         "region_name": "region_name"
        ...     }
        ... ))

    See Also
    -------
    zarr.storage
    """

    _root: Optional[Group] = None
    _fs: Optional[FSStore] = None
    sep = "/"
    DEFAULT_COMPRESSOR = Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)

    # docstr-coverage: inherited
    def __init__(self, url: str) -> None:
        super().__init__(url)
        self._delayed_list: list[Delayed] = []
        self._zarr_kwargs: dict[str, Any] = dict()
        self._dask_kwargs: dict[str, Any] = dict()

    # docstr-coverage: inherited
    def open(self, mode: str = "r", consolidated: bool = True, **kwargs: Any) -> None:
        """Open the store in the given mode

        library specifics parameters :
            - compressor : numcodecs compressor. ex : Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)

        Parameters
        ----------
        mode: str, optional
            mode to open the store
        consolidated: bool
            in reading mode, indicate if consolidate metadata file should be use or not
        **kwargs: Any
            extra kwargs of open on librairy used.
        """
        super().open()
        self._mode = mode
        # dask can take specific kwargs (and probably zarr too).
        kwargs.setdefault("zarr_kwargs", dict())
        kwargs.setdefault("dask_kwargs", dict())
        dask_kwargs = kwargs["dask_kwargs"]
        dask_kwargs.setdefault("storage_options", dict())
        if not mode.startswith("r") or "+" in mode:
            dask_kwargs.setdefault("compressor", self.DEFAULT_COMPRESSOR)
            dask_kwargs.setdefault("compute", False)
            dask_kwargs.setdefault("overwrite", True)

        self._zarr_kwargs = kwargs.pop("zarr_kwargs")
        self._dask_kwargs = kwargs.pop("dask_kwargs")
        self._zarr_kwargs.update(**kwargs)
        self._dask_kwargs.update(**kwargs)

        if mode == "r" and consolidated:
            self._root: Group = zarr.open_consolidated(store=self.url, mode=mode, **self._zarr_kwargs)
        else:
            self._root: Group = zarr.open(store=self.url, mode=mode, **self._zarr_kwargs)
        self._fs = self._root.store

    # docstr-coverage: inherited
    def close(self) -> None:
        super().close()

        if not isinstance(self._root, Group):
            raise StoreNotOpenError("Store must be open before close it")

        if len(self._delayed_list) > 0:
            dask.compute(self._delayed_list)
        self._delayed_list = []

        # only if we write
        if any(self._mode.startswith(mode) for mode in ("w", "a")) or "+" in self._mode:
            zarr.consolidate_metadata(self._root.store)

        self._root = None
        self._fs = None

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return contains_group(self._fs, path=path)

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        if self._fs is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return contains_array(self._fs, path=path)

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self._root[group_path].attrs.update(conv(attrs))

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter(self._root.get(path, []))

    def __getitem__(self, key: str) -> "EOObject":
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")

        from eopf.product.core import EOGroup, EOVariable

        obj = self._root[key]
        if self.is_group(key):
            return EOGroup(attrs=obj.attrs)
        # Use dask instead of zarr to read the object data to :
        # - avoid memory leak/let dask manage lazily close the data file
        # - read in parallel
        var_data = da.from_zarr(self.url, component=key, storage_options=self._dask_kwargs["storage_options"])

        # apply scale, offset and fill value masking
        if "scale_factor" in obj.attrs:
            scale_factor = obj.attrs["scale_factor"]
        else:
            scale_factor = 1
        if "add_offset" in obj.attrs:
            offset = obj.attrs["add_offset"]
        else:
            offset = 0
        fill = False
        try:
            if "_FillValue" in obj.attrs.keys() and not da.isnan(obj.attrs["_FillValue"]):
                fill = True
        except TypeError:
            # avoid printing warnings about not beeing able to detect and convert NaN
            pass
        var_data = da.where(
            fill and var_data == obj.attrs["_FillValue"],
            var_data,
            var_data * da.asarray(scale_factor) + offset,
        )

        return EOVariable(data=var_data, attrs=obj.attrs)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        from eopf.product.core import EOGroup, EOVariable

        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if isinstance(value, EOGroup):
            self._root.create_group(key, overwrite=True)
        elif isinstance(value, EOVariable):
            data = value.data

            # apply scale, offset and fill value masking
            if "scale_factor" in value.attrs:
                scale_factor = value.attrs["scale_factor"]
            else:
                scale_factor = 1
            if "add_offset" in value.attrs:
                offset = value.attrs["add_offset"]
            else:
                offset = 0
            fill = False
            try:
                if "_FillValue" in value.attrs.keys() and not da.isnan(value.attrs["_FillValue"]):
                    fill = True
            except TypeError:
                # avoid printing warnings about not beeing able to detect and convert NaN
                pass
            data = da.where(
                fill and data == value.attrs["_FillValue"],
                data,
                (data - offset) / scale_factor,
            )

            dask_array = da.asarray(data, dtype=value.data.dtype)  # .data is generally already a dask array.
            if dask_array.size > 0:
                # We must use to_zarr for writing on a distributed cluster,
                # but to_zarr fail to write array with a 0 dim (divide by zero Exception)
                delayed = dask_array.to_zarr(self.url, component=key, **self._dask_kwargs)
                if delayed is not None:
                    self._delayed_list.append(delayed)
            else:
                self._root.create(key, shape=dask_array.shape)
        else:
            raise TypeError("Only EOGroup and EOVariable can be set")
        self.write_attrs(key, value.attrs)

    def __delitem__(self, key: str) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        del self._root[key]

    def __len__(self) -> int:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return len(self._root)

    def __iter__(self) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return iter(self._root)

    # docstr-coverage: inherited
    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".zarr", ".zip", ""]
