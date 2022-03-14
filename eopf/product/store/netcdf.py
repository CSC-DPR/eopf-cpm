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

    """

    RESTRICTED_ATTR_KEY = ("_FillValue",)

    # docstr-coverage: inherited
    def __init__(self, url: str) -> None:
        """Instantiate based on the url(file path) of the manifest netCDF4 file

        Parameters
        ----------
        url: str,
            path to netCDF4 file
        """
        url = os.path.expanduser(url)
        super().__init__(url)
        self._root: Optional[Dataset] = None
        # Set default values for compression arguments
        # Compression level can be set within range [0, 9]
        self.zlib: bool = True
        self.complevel: int = 4
        self.shuffle: bool = True
        # Scale is used to enable/disable automatic variable scaling
        self.scale: bool = False

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:
        """Open the store in the given mode

        Parameters
        ----------
        mode: str, optional, default set to "read"
            mode to open the store
        **kwargs: Any
            extra kwargs of open on library used
        """
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

    # docstr-coverage: inherited
    def close(self) -> None:
        """Close the store"""
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        super().close()
        self._root.close()
        self._root = None

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        """
        This method is used to verify if a given path is a representation of a group
        """
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(path)
        return isinstance(current_node, (Group, Dataset))

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        """
        This method is used to verify if a given path is a representation of a variable
        """
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(path)
        return isinstance(current_node, Variable)

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        """
        This method is used to update attributes in the store
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

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        """Iter over the given path

        Parameters
        ----------
        path: str
            path to the object to iterate over

        Returns
        -------
        Iterator[str]
            An iterator of the paths inside the given path

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current_node = self._select_node(path)
        return it.chain(iter(current_node.groups), iter(current_node.variables))

    def __getitem__(self, key: str) -> "EOObject":
        """Getter for netCDF4 variable or group

        Parameters
        ----------
        key: str
            Name of group or variable
        Raises
        ------
        StoreNotOpenError
            If the store is closed
        Returns
        ----------
        EOObject
        """
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

    def __setitem__(self, key: str, value: "EOObject") -> None:
        """Setter for netCDF4 variable or group

        Parameters
        ----------
        key: str
            Name of group or variable
        value: EOObject
            Value of EOObject
        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """
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

    def __len__(self) -> int:
        """Used to return total length of an EOOBject

        Returns
        ----------
        Int
        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return len(self._root.groups) + len(self._root.variables)

    def __iter__(self) -> Iterator[str]:
        """Iterator over each variable based on its group

        Returns
        ----------
        An iterator over groups and variables
        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """
        if self._root is None:
            raise StoreNotOpenError("Store must be open before access to it")
        return it.chain(iter(self._root.groups), iter(self._root.variables))

    def _select_node(self, key: str) -> Union[Dataset, Group, Variable]:
        """Getter for netCDF4 Dataset Variable

        Returns
        Union of Dataset, group, variable
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
