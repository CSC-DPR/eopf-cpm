import itertools as it
import os
import pathlib
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Optional, Union

import fsspec
import xarray as xr
from netCDF4 import Dataset, Group, Variable

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore
from eopf.product.utils import conv, decode_attrs, reverse_conv

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


class EONetCDFStore(EOProductStore):
    """
    Store representation to access NetCDF format of the given URL

    Parameters
    ----------
    url: str
        path url or the target store

    Attributes
    ----------
    url: str
        path url or the target store
    zlib: bool
        enable/disable compression
    complevel: int [1-9]
        level of the compression
    shuffle: bool
        enable/disable hdf5 shuffle
    """

    RESTRICTED_ATTR_KEY = ("_FillValue",)

    # docstr-coverage: inherited
    def __init__(self, url: str) -> None:
        url = os.path.expanduser(url)
        super().__init__(url)
        self._root: Optional[Dataset] = None
        self.zlib: bool = True
        self.complevel: int = 4
        self.shuffle: bool = True
        self._local_url: Optional[str] = None

    def __getitem__(self, key: str) -> "EOObject":

        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")

        from eopf.product.core import EOGroup, EOVariable

        try:
            obj = self._select_node(key)
        except IndexError as e:  # if key is invalid, netcdf4 raise IndexError ...
            raise KeyError(e)
        attrs = {key: decode_attrs(value) for key, value in obj.__dict__.items()}
        if self.is_group(key):
            return EOGroup(attrs=attrs)
        return EOVariable(data=obj[:], attrs=attrs, dims=obj.dimensions)

    def __iter__(self) -> Iterator[str]:

        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return it.chain(iter(self._root.groups), iter(self._root.variables))

    def __len__(self) -> int:

        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return len(self._root.groups) + len(self._root.variables)

    def __setitem__(self, key: str, value: "EOObject") -> None:

        from eopf.product.core import EOGroup, EOVariable

        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if isinstance(value, EOGroup):
            self._root.createGroup(key)
            self.write_attrs(key, value.attrs)
        elif isinstance(value, EOVariable):
            # Recover / create dimensions from target product
            for idx, dim in enumerate(value.dims):
                if dim not in self._root.dimensions:
                    self._root.createDimension(dim, size=value._data.shape[idx])
                if len(self._root.dimensions[dim]) != value._data.shape[idx]:
                    raise ValueError(
                        "Netdf4 format does not support mutiples dimensions with the same name and different size.",
                    )
            # Create and write EOVariable
            variable = self._root.createVariable(
                key,
                value.data.dtype,
                dimensions=value.dims,
                zlib=self.zlib,
                complevel=self.complevel,
                shuffle=self.shuffle,
            )
            self.write_attrs(key, value.attrs, value.data.dtype)
            variable[:] = value._data.values
        else:
            raise TypeError("Only EOGroup and EOVariable can be set")

    # docstr-coverage: inherited
    def close(self) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        super().close()
        self._root.close()
        self._root = None

    # docstr-coverage: inherited
    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        """
            Determines if a given file path can be read with the current store
        Parameters
        ----------
        file_path: str
            Path to netCDF4 file
        Return
        ------
        Boolean
        """
        return pathlib.Path(file_path).suffix in [".nc"]

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:

        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(path)
        return isinstance(current_node, (Group, Dataset))

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:

        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(path)
        return isinstance(current_node, Variable)

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:

        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(path)
        return it.chain(iter(current_node.groups), iter(current_node.variables))

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:

        super().open()
        # Overwrite compression / scale parameters if given by user
        self.zlib = bool(kwargs.pop("zlib", True))
        self.complevel = int(kwargs.pop("complevel", 4))
        self.shuffle = bool(kwargs.pop("shuffle", True))

        # We can't open a netcdf4 Dataset on a python File like cf :
        # https://github.com/Unidata/netcdf4-python/issues/295
        # This is a problem as fsspec, notably used to get data from a S3, return us a File like.
        # Possible workaround :
        # - initialising the dataset with memory = File bynary data, but it's neither lazy nor memory efficient
        # - use filecache to make a temporary local copy
        if self.url.startswith("s3:"):
            # Make a temporary copy. According to the doc its supposed to get deleted at the end of the process.
            # I have some serious doubt that it's working, we might want to manually delete it,
            # but it's not safe if multiple store access the same file.
            self._local_url = fsspec.open_local(
                "filecache::" + self.url, mode, **kwargs["storage_options"], filecache={"cache_storage": "TMP"}
            )
        else:
            self._local_url = self.url
        self._root = Dataset(self._local_url, mode, **kwargs)

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}, data_type: Any = int) -> None:
        """
        This method is used to update attributes in the store

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(group_path)
        from json import dumps
        from numbers import Number

        conv_attr: MutableMapping[str, Any] = {}
        for attr, value in attrs.items():
            attr_value = value
            if attr not in self.RESTRICTED_ATTR_KEY:
                if not isinstance(value, Number):
                    attr_value = dumps(conv(attr_value))
            elif type(attr_value) is not data_type:
                attr_value = reverse_conv(data_type, attr_value)
            conv_attr[attr] = attr_value
        current_node.setncatts(conv_attr)

    def _select_node(self, key: str) -> Union[Dataset, Group, Variable]:
        """Retrieve and return the netcdf4 object corresponding to the node at the given path

        Returns
        ----------
        Union of Dataset, Group, Variable

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if key in ["/", ""]:
            return self._root
        return self._root[key]


class EONetcdfStringToTimeAccessor(EOProductStore):
    """
    Store representation to access NetCDF date time format of the given URL

    Parameters
    ----------
    url: str
        path url or the target store
    """

    # docstr-coverage: inherited
    def __init__(self, url: str) -> None:
        url = os.path.expanduser(url)
        super().__init__(url)
        self._root = None

    def __getitem__(self, key: str) -> "EOObject":
        import pandas as pd

        from eopf.product.core import EOVariable

        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")

        # convert unix start time to date time format
        time_da = self._root.get(key, "1970-1-1T0:0:0.000000Z")
        start = pd.to_datetime("1970-1-1T0:0:0.000000Z")
        end = pd.to_datetime(time_da)
        # compute and convert the time difference into microseconds
        time_delta = (end - start) // pd.Timedelta("1microsecond")

        # create coresponding attributes
        attributes = {}
        attributes["unit"] = "microseconds since 1970-1-1T0:0:0.000000Z"
        attributes["standard_name"] = "time"
        if key == "ANX_time":
            attributes["long_name"] = "Time of ascending node crossing in UTC"
        elif key == "calibration_time":
            attributes["long_name"] = "Time of calibration in UTC"

        # create an EOVariable and return it
        eov: EOVariable = EOVariable(data=time_delta, attrs=attributes)
        return eov

    def __iter__(self) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        yield from ()

    def __len__(self) -> int:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return 1

    def __setitem__(self, key: str, value: "EOObject") -> None:
        from eopf.product.core import EOVariable

        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        # set the data
        if not isinstance(value, EOVariable):
            raise TypeError(f"The value {key} must be an EOVariable")
        self._check_node(key)

        self._root[key] = value._data
        # set the attrs of the value
        self.write_attrs(key, value.attrs)
        # write to netcdf
        self._root.to_netcdf(self.url)

    # docstr-coverage: inherited
    def close(self) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        super().close()
        self._root.close()
        self._root = None

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")

        return True

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")

        return False

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self._check_node(path)
        return iter([])

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        self._root = xr.open_dataset(self.url, mode=mode)

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        self._check_node(group_path)
        self._root.attrs.update(attrs)

    def _check_node(self, key: str) -> Union[Dataset, Group, Variable]:
        """Check if the key exists, only top level is used

        Returns
        ----------
        Union of Dataset, Group, Variable

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        KeyError
            If the key does not exist
        """
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if key not in ["/", ""]:
            raise KeyError(f"{key} does not exist")
