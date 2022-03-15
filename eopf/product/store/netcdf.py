import itertools as it
import os
import pathlib
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Optional, Union

from netCDF4 import Dataset, Group, Variable

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore
from eopf.product.utils import conv, decode_attrs

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
    scale: bool
        enable/disable automatic variable scaling
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
        self.scale: bool = False

    def __getitem__(self, key: str) -> "EOObject":

        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")

        from eopf.product.core import EOGroup, EOVariable

        try:
            obj = self._select_node(key)
        except IndexError as e:  # if key is invalid, netcdf4 raise IndexError ...
            raise KeyError(e)
        if self.is_group(key):
            return EOGroup(attrs=decode_attrs(obj.__dict__))
        return EOVariable(data=obj, attrs=decode_attrs(obj.__dict__), dims=obj.dimensions)

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
        elif isinstance(value, EOVariable):
            dimensions = []
            # FIXME: dimensions between value._data and value can mismatch ...
            # Recover / create dimensions from target product
            for idx, (dim, _) in enumerate(zip(value.dims, value._data.dims)):
                if dim not in self._root.dimensions:
                    self._root.createDimension(dim, size=value._data.shape[idx])
                dimensions.append(dim)
            # Create and write EOVariable
            variable = self._root.createVariable(
                key,
                value._data.dtype,
                dimensions=dimensions,
                zlib=self.zlib,
                complevel=self.complevel,
                shuffle=self.shuffle,
            )
            variable.set_auto_scale(self.scale)
            variable[:] = value._data.values
        else:
            raise TypeError("Only EOGroup and EOVariable can be set")
        self.write_attrs(key, value.attrs)

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
        if "zlib" in kwargs:
            self.zlib = bool(kwargs.get("zlib"))
            kwargs.pop("zlib")
        if "complevel" in kwargs:
            self.complevel = int(str(kwargs.get("complevel")))
            kwargs.pop("complevel")
        if "shuffle" in kwargs:
            self.shuffle = bool(kwargs.get("shuffle"))
            kwargs.pop("shuffle")
        if "scale" in kwargs:
            self.scale = bool(kwargs.get("scale"))
            kwargs.pop("scale")
        self._root = Dataset(self.url, mode, **kwargs)

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
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

        # convert attributes to python data types
        # if not a number convert to json dict
        # since netCDF4 does not allow dictionary imbrication
        if group_path == "/" or group_path == "":
            attrs = {attr: dumps(conv(value)) for attr, value in attrs.items() if attr not in self.RESTRICTED_ATTR_KEY}
        else:
            attrs = {attr: conv(value) for attr, value in attrs.items() if attr not in self.RESTRICTED_ATTR_KEY}

        current_node.setncatts(attrs)

    def _select_node(self, key: str) -> Union[Dataset, Group, Variable]:
        """Retrieve and return the netcdf4 object corresponding to the node at the given path

        Returns
        Union of Dataset, Group, Variable
        ----------
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
