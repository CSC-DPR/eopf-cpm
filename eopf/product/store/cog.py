import os
import pathlib
from collections.abc import MutableMapping
from json import loads
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Optional

import fsspec
import rasterio
import rioxarray
import xarray

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import StorageStatus
from eopf.product.store.abstract import EOProductStore
from eopf.product.store.netcdf import EONetCDFStore

if TYPE_CHECKING:  # pragma: no cover
    from distributed import Lock

    from eopf.product.core.eo_object import EOObject


class EOCogStore(EOProductStore):
    # Wrapper class
    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.url = url
        self._ds = None

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        if self.url.startswith("s3:"):
            self._sub_store = self._open_cloud(mode, **kwargs)
        else:
            self._sub_store = self._open_local(mode, **kwargs)
        super().open(mode)

    def _open_local(self, mode: str = "r", **kwargs: Any) -> "EOCogStoreLOCAL":
        sub_store = EOCogStoreLOCAL("some_local_url")
        sub_store.open(mode, **kwargs)
        return sub_store

    def _open_cloud(self, mode: str = "r", **kwargs: Any) -> "EOCogStoreLOCAL":
        storage_options = kwargs.pop("storage_options", dict())
        self.url = fsspec.open_local("filecache::" + self.url, mode, s3=storage_options, filecache={"cache_storage": "TMP"})

        sub_store = EOCogStoreLOCAL(self.url)
        sub_store.open(mode, **kwargs)
        return sub_store

    def is_group(self, path: str) -> bool:
        return self._sub_store.is_group(path)

    def is_variable(self, path: str) -> bool:
        return self._sub_store.is_variable(path)

    def __len__(self) -> int:
        return len(self._sub_store)

    def iter(self, path: str) -> Iterator[str]:
        return iter("")

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = ...) -> None:
        return self._sub_store.write_attrs(group_path, attrs)

    def __getitem__(self, key: str) -> "EOObject":
        if key in ["", "/"]:
            if self._ds is None:
                self._ds = rioxarray.open_rasterio(self.url)
            return self._ds
        # More logic to be added for folder
        
    def __setitem__(self, key: str, value: "EOObject") -> None:
        self._sub_store[key] = value

class EOCogStoreLOCAL(EOProductStore):
    """
    Accessor representation to access Raster like jpg2000 or tiff.

    Parameters
    ----------
    url: str
        path or url to access
    """

    sep = "/"
    expected_raster_dims = [("band", "x", "y"), ("band", "y", "x"), ("x", "y"), ("y", "x"), ("rows", "columns")]
    attributes_file_name = "attrs.json"

    def __init__(self, url: str, **kwargs) -> None:
        super().__init__(url)
        self._mode: Optional[str] = None
        self._lock: Optional[Lock] = None
        self._opened: bool = False

    def __getitem__(self, key: str) -> "EOObject":
        """
        This method is used to read EOVariable or EOGroup from Store

        Parameters
        ----------
        key: str
            path

        Raise
        ----------
        StoreNotOpenError, when store is not open
        NotImplementedError, when open mode is not r(ead)

        Return
        ----------
        EOVariable / EOGroup
        """
        from eopf.product.core import EOVariable

        if self.status == StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        if self._mode != "r":
            raise NotImplementedError("Only available in reading mode")

        if key in ["", "/"]:
            key_path = Path(self.url).resolve()
        else:
            # Temporary solution, to be updated / improved with fsspec
            # key_path = Path(self.url).resolve()  /  key.removeprefix(self.sep)
            key_path = Path(self.url + "/" + key).resolve()

        # check if key with extension (nc or cog) is file, and return EOV
        # Try EOV and both extensions
        for suffix in [".nc", ".cog"]:
            if key_path.with_suffix(suffix).is_file():
                eov_name = key_path.stem
                eov = self._read_eov(key_path.with_suffix(suffix), eov_name)
                return EOVariable(eov_name, eov)
        # Read directory and return EOGroup
        if key_path.is_dir():
            return self._read_dir(key_path)

        # KeyError if key is not EOV or EOG
        raise KeyError(f"Given {key_path} was not found!")

    def __iter__(self) -> Iterator[str]:
        """Has no functionality within this store"""
        return self.iter("")

    def __len__(self) -> int:
        """Has no functionality within this store"""
        if self.status == StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        if self._mode != "r":
            raise NotImplementedError("Only available in reading mode")
        return sum(1 for _ in self.iter(""))

    def __setitem__(self, key: str, value: "EOObject") -> None:
        """
        This metod is used to write an EOObject
        Parameters
        ----------
        key: str
            Path of variable to be written
        value: EOObject
            EOObject data to be written

        Raise
        ----------
        StoreNotOpenError, if store is not open
        NotImplementedError, if mode is not w(rite) / value is not EOGroup or EOVariable
        """
        from pathlib import Path

        from eopf.product.core import EOGroup, EOVariable

        init_path = Path(self.url)

        if self.status == StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        if self._mode != "w":
            raise NotImplementedError("Only available in writing mode")
        if isinstance(value, EOVariable) or isinstance(value, xarray.DataArray):
            self._write_eov(value, init_path, key)
        elif isinstance(value, EOGroup):
            # if the key starts with / the join will not be carried
            key = key.removeprefix(self.sep)
            output_dir = init_path / key
            if not output_dir.is_dir():
                output_dir.mkdir(parents=True, exist_ok=True)
            # write the attrbutes of the group
            self.write_attrs(str(output_dir), value.attrs)
            # iterate trough all variables of the EOGroup
            # and write each in one file, cog or netCDF4
            for var_name, var_val in value.variables:
                self._write_eov(var_val, output_dir, var_name)
        else:
            raise NotImplementedError()

    # docstr-coverage: inherited
    def close(self) -> None:
        if self.status == StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        super().close()
        self._mode = None
        self._lock = None
        self._opened = False

    # docstr-coverage: inherited
    @property
    def is_erasable(self) -> bool:
        return False

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        if self._mode == "w":
            raise NotImplementedError("Only available in reading mode")
        path = path.removeprefix(self.sep)
        url_path = Path(self.url).resolve()
        sub_path = url_path / path
        # check if sub_path is a directory
        return sub_path.is_dir()

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        # check if it is a file or not
        if self._mode == "w":
            raise NotImplementedError("Only available in reading mode")
        path = path.removeprefix(self.sep)
        url_path = Path(self.url).resolve()
        sub_path = url_path / path
        # check if sub_path is a directory
        return sub_path.is_file()

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        if self._mode == "w":
            raise NotImplementedError("Only available in reading mode")
        # Get the full path of requested iterator
        iter_path = Path(self.url).resolve() / path.removeprefix(self.sep)
        for item in os.listdir(iter_path):
            item_path = Path(iter_path / item)
            # check if item is directory or an accepted file (.cog, .nc)
            if item_path.is_dir() or self.guess_can_read(item):
                # remove suffix to match variable name
                yield item.removesuffix(item_path.suffix)

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open(mode=mode)
        self._mode = mode

        self._lock = kwargs.get("lock")
        if mode in ["r", "w"]:
            self._opened = True
        else:
            raise ValueError("Unsuported mode, only (r)ead or (w)rite")

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        from json import dump

        if self._mode == "r":
            raise NotImplementedError("Only available in writing mode")

        # if there are group attributes write then as json file inside the group
        if attrs:
            init_path = pathlib.Path(self.url).resolve()
            # # the file should be written in the directory representing the group
            attrs_file_path = init_path / group_path / self.attributes_file_name
            # remove file if it already exists
            if attrs_file_path.is_file():
                attrs_file_path.unlink()
            # write the json file
            with open(attrs_file_path, "w") as fp:
                dump(attrs, fp)

    def _write_cog(self, value: Any, file_path: pathlib.Path) -> None:
        """
        This method is used to write rasters to .cog files

        Parameters
        ----------
        value: Any
            rasterIO dataset
        file_path: pathlib.Path
            Path to .cog file
        """
        # set suffix .cog and transfrom to absolute path
        cog_path = file_path.with_suffix(".cog")
        abs_cog_path = cog_path.resolve()

        # if the dimension names are not x,y we need to let
        # rioxarray know which dimension is x and y
        if len(value.dims) == 2 and (value.dims[0] != "y" or value.dims[1] != "x"):
            value._data.rio.set_spatial_dims(x_dim=value.dims[1], y_dim=value.dims[0], inplace=True)
        # write the COG file
        if isinstance(value, xarray.DataArray):
            value.rio.to_raster(abs_cog_path, tiled=True, lock=self._lock, driver="COG")
        else:
            # EOVariable
            value._data.rio.to_raster(abs_cog_path, tiled=True, lock=self._lock, driver="COG")

    def _write_netCDF4(self, value: "EOObject", file_path: pathlib.Path, var_name: str) -> None:
        """
        This method is used to write rasters to .nc (netcdf) files

        Parameters
        ----------
        value: Any
            rasterIO dataset
        file_path: pathlib.Path
            Path to .nc file
        var_name: str
            Name of EOVariable
        """
        # set suffix .nc and transfrom to absolute path (path-like string)
        nc_path = file_path.with_suffix(".nc")
        abs_nc_path = str(nc_path.resolve())

        # write the netCDF4 file
        nc = EONetCDFStore(abs_nc_path)
        nc.open(mode="w")
        nc[var_name] = value
        nc.close()

    def _is_raster(self, value: Any) -> bool:
        """
        This metod is used if dataset dimensions match with a standard format.
        Parameters
        ----------
        value: Any
            rasterIO dataset
        """
        return value.dims in self.expected_raster_dims

    def _write_eov(self, value: "EOObject", output_dir: pathlib.Path, var_name: str) -> None:
        """
        This metod is used to write an EOVariable to cog or netcdf format.
        Parameters
        ----------
        value: EOVariable
            Variable to be written
        outputdir: pathlib.Path
            Path to output folder
        var_name: str
            Name of EOVariable
        """
        var_name = var_name.removeprefix(self.sep)
        file_path: pathlib.Path = output_dir / var_name

        # determine if variable is a raster, i.e. can be written as cog
        if self._is_raster(value):
            self._write_cog(value, file_path)
        else:
            self._write_netCDF4(value, file_path, os.path.basename(var_name))

    def _read_eov(self, file: Path, eov_name: str) -> xarray.DataArray:
        """
        This metod is used to read a file and return it as rasterio dataset.
        Parameters
        ----------
        file: pathlib.Path
            Path to input .cog or .nc file
        eov_name: str
            Name of EOVariable
        Raise
        ----------
        TypeError, when input file cannot be read or converted by rasterrio

        Return
        ----------
        xarray.DataArray
        """
        try:
            # Return rasterio dataset for .cog and .nc files.
            return rioxarray.open_rasterio(file)
        except rasterio.errors.RasterioIOError:
            # try to reopen using netcdf scheme identifier
            return rioxarray.open_rasterio(f"netcdf:{file}:{eov_name}")
        except Exception as e:
            # this should be another error type
            raise TypeError(f"Can NOT read: {file}", e)

    def _read_attrs(self, dir_path: Path) -> dict[str, Any]:
        """
        This metod is used to read json attributes of an EOGroup.
        Parameters
        ----------
        dir_path: pathlib.Path
            Path to input .json file
        Return
        ----------
        dict[str, Any]
        """
        attrs_path = dir_path / self.attributes_file_name
        if attrs_path.is_file():
            with open(attrs_path, "r") as fp:
                return loads(fp.read())
        else:
            return {}

    def _read_dir(self, dir_path: Path) -> "EOObject":
        """
        This metod is used to create and EOGroup at a given path.
        Parameters
        ----------
        dir_path: pathlib.Path
            Path to directory to be converted
        Return
        ----------
        EOObject
        """
        from eopf.product.core import EOGroup

        return EOGroup(attrs=self._read_attrs(dir_path))

    # docstr-coverage: inherited
    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".cog", ".nc"]
