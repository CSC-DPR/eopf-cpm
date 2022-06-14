import os
import pathlib
from collections.abc import MutableMapping
from json import loads
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Optional

import boto3
import fsspec
import rasterio
import rioxarray
import xarray
from rasterio.session import AWSSession

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import StorageStatus
from eopf.product.store.abstract import EOProductStore
from eopf.product.store.netcdf import EONetCDFStore

if TYPE_CHECKING:  # pragma: no cover
    from distributed import Lock

    from eopf.product.core.eo_object import EOObject


class EOCogStore(EOProductStore):
    # Wrapper class
    attributes_file_name = "attrs.json"
    sep = "/"

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.url: fsspec.core.OpenFile = url
        self._ds = None
        self._sub_store: Any = None
        self._is_zip = False
        self.mapper: fsspec.mapping.FSMap = None

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        self._mode: str = mode
        # Use wrapper-class if target path is an S3, otherwise, use normal CogStore
        if self.url.startswith("s3:") or self.url.startswith("zip::s3:"):
            self._open_cloud(mode, **kwargs)
        else:
            self._sub_store = self._open_local(mode, **kwargs)
        super().open(mode)

    def _open_local(self, mode: str = "r", **kwargs: Any) -> "EOCogStoreLOCAL":
        sub_store = EOCogStoreLOCAL(self.url)
        sub_store.open(mode, **kwargs)
        return sub_store

    def _open_cloud(self, mode: str = "r", **kwargs: Any) -> None:
        self.storage_options = kwargs.pop("storage_options", dict())
        # To be fixed, bypass to eop.load() error when endpoint does not contain https
        new_endpoint = self.storage_options["client_kwargs"]["endpoint_url"].replace("https://", "")
        local_cfg = dict(
            key=self.storage_options["key"],
            secret=self.storage_options["secret"],
            client_kwargs=dict(
                endpoint_url=new_endpoint,
                region_name=self.storage_options["client_kwargs"]["region_name"],
            ),
        )

        # Initialize s3 session
        self._session = boto3.Session(
            aws_access_key_id=local_cfg["key"],
            aws_secret_access_key=local_cfg["secret"],
            region_name=local_cfg["client_kwargs"]["region_name"],
        )
        self._raster_env = rasterio.Env(
            AWSSession(self._session, endpoint_url=local_cfg["client_kwargs"]["endpoint_url"]),
            AWS_VIRTUAL_HOSTING="False",
        )
        # Load URL, according to target, Standard or ZIP
        if self.url.startswith("zip"):
            # for ZIP, opened with a dict(s3=credentials)
            self._url_name = self.url.replace("zip::", "")
            self.url = fsspec.open(self.url, mode, s3=self.storage_options)
            self._is_zip = True
        else:
            # For standard storage, opened with a hasable dict.
            self.url = fsspec.open(self.url, mode, **self.storage_options)
        # Get mapper
        self.mapper = self.url.fs.get_mapper(self.url.path)

    def is_group(self, path: str) -> bool:
        if self._sub_store is not None:
            return self._sub_store.is_group(path)

        if self._is_zip:
            # Check if path is a directory inside ZIP
            return self.mapper.fs.isdir(path)
        # Compose ful path and check if it is a directory
        group_path = self.url.path + self.sep + path
        return self.url.fs.isdir(group_path)

    def is_variable(self, path: str) -> bool:
        if self._sub_store is not None:
            return self._sub_store.is_variable(path)

        if self._is_zip:
            # Check if (non composed) key is found inside ZIP file
            # tbd? return path in self.mapper.keys()
            for extension in [".cog", ".nc"]:
                if self.mapper.fs.isfile(self.add_extension(path, extension)):
                    return True
            return False
        # For standard S3, compose full path, and check if it's a file
        file_path = self.url.path + self.sep + path
        return self.url.fs.isfile(file_path)

    def __len__(self) -> int:
        if self._sub_store is not None:
            return len(self._sub_store)
        else:
            return sum(1 for _ in self.iter(""))

    def iter(self, path: str) -> Iterator[str]:
        # Return sub_store iter if it's set
        if self._sub_store is not None:
            yield from self._sub_store.iter(path)
            return
        # Compose key for top level group (should work for zip also)
        if self._is_zip:
            if path in ["", "/"]:
                path = "*"
            else:
                path = f"{path}/*"
        else:
            if path in ["", "/"]:
                path = f"{self.mapper.root}/*"
            else:
                path = f"{self.mapper.root}{path}/*"

        for item in self.mapper.fs.glob(path):
            if self.mapper.fs.isdir(item) or self._guess_can_read_files(item):
                item = item.removesuffix(self.sep)
                # Remove file extension
                for extension in [".cog", ".nc"]:
                    item = item.removesuffix(extension)
                yield item.split(self.sep)[-1]

    def write_attrs(self, group_path: str, attrs: Any = ...) -> None:
        return self._sub_store.write_attrs(group_path, attrs)

    def __getitem__(self, key: str) -> "EOObject":
        if self._sub_store is not None:
            return self._sub_store[key]

        if not self._is_zip:
            return self._standard_getitem(key)
        else:
            return self._zip_getitem(key)

    def _standard_getitem(self, key: str) -> "EOObject":
        # Standard S3 Storage, directory hierarchy
        from eopf.product.core import EOGroup, EOVariable

        if key in ["", "/"]:
            return EOGroup(attrs=self._read_attrs(key))
        # Remove // if needed and compose full path (mapper.root + key)
        key = key.removeprefix("//")
        key_path = f"{self.mapper.root}/{key}"
        # Return attributes if key is path to directory (group)
        if self.is_group(key):
            return EOGroup(attrs=self._read_attrs(key))
        # If key is path to .nc or .cog file, build data and return EOV
        # guess_can_read() is needed since key can be <<group/variable>> or <<group/variable.cog>>
        if self._guess_can_read_files(key_path):
            if self.is_variable(key):
                var_name, var_data = self._read_eov(key)
                return EOVariable(var_name, data=var_data)
        else:
            # If cannot be read, try to append a accepted extension, and check if file exist
            for suffix in [".nc", ".cog"]:
                if self.is_variable(key + suffix):
                    # If composed variable path is found, build data and return EOV
                    var_name, var_data = self._read_eov(key + suffix)
                    return EOVariable(var_name, data=var_data)
        raise KeyError(f"{key_path} not found!")

    def _zip_getitem(self, key: str) -> "EOObject":
        from eopf.product.core import EOGroup, EOVariable

        # ZIPed target
        # For top level or directories, return attributes
        if key in ["", "/"] or self.mapper.fs.isdir(key):
            return EOGroup(attrs=self._read_attrs(key))
        # if key is in mapper, build data and return EOV
        for suffix in [".nc", ".cog"]:
            if key + suffix in self.mapper.keys():
                var_name, var_data = self._read_eov(key + suffix)
                return EOVariable(var_name, data=var_data)
        raise KeyError(f"{key} not found!")

    def __setitem__(self, key: str, value: "EOObject") -> None:
        if self._sub_store is not None:
            self._sub_store[key] = value

    # docstr-coverage: inherited
    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        # To be added .ZIP
        return pathlib.Path(file_path).suffix in [".cogs"]

    @staticmethod
    def _guess_can_read_files(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".nc", ".cog"]

    def _read_attrs(self, path: str) -> dict[str, Any]:
        if self._sub_store is not None:
            return self._sub_store._read_attrs(path)

        import json

        # if path/attrs.json is file, read and return it as a dict, else return an empty dict
        if self.mapper.fs.isfile(os.path.join(path, self.attributes_file_name)):
            return json.loads(self.mapper[self.attributes_file_name])
        else:
            return {}

    def _read_eov(self, path: str) -> tuple[str, Any]:
        # Create variable name by removing extension if exists

        variable_name: Optional[str] = None
        if self._guess_can_read_files(path):
            variable_name = self.remove_extension(path.split("/")[-1])
        else:
            # Check if file is cog or netcdf, raise valueError otherwise
            for extension in [".cog", ".nc"]:
                if self.mapper.fs.isfile(self.add_extension(path, extension)):
                    variable_name = path.split("/")[-1]
                    path = self.add_extension(path, extension)

        if not variable_name:
            raise ValueError(f"{path=} is not a valid one")

        # Compose fullpath scheme
        if not self._is_zip:
            full_file_path = f"s3://{self.mapper.root}/{path}"
        else:
            full_file_path = f"zip+{self._url_name}!{path}"

        with self._raster_env:
            try:
                variable_data = xarray.open_dataset(full_file_path, engine="rasterio", chunks="auto")
                return variable_name, variable_data["band_data"]
            except ValueError:
                # Use netcdf for files that cannot be read with xarray
                if not self._is_zip:
                    data = EONetCDFStore(full_file_path)
                    data.open(storage_options=self.storage_options)
                    variable_data = data[variable_name]
                    return variable_name, variable_data
                else:
                    # To be added for zips
                    raise NotImplementedError()

    @staticmethod
    def remove_extension(path: str) -> str:
        for suffix in [".cog", ".nc"]:
            path = path.removesuffix(suffix)
        return path

    @staticmethod
    def add_extension(path: str, extension: str) -> str:
        return path + extension


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

    def __init__(self, url: str, **kwargs: Any) -> None:
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
            key_path = Path(f"{self.url}/{key}").resolve()

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
        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
        if self._mode == "w":
            raise NotImplementedError("Only available in reading mode")
        path = path.removeprefix(self.sep)
        url_path = Path(self.url).resolve()
        sub_path = url_path / path
        # check if sub_path is a directory
        return sub_path.is_dir()

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
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
        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
        if self._mode == "w":
            raise NotImplementedError("Only available in reading mode")
        # Get the full path of requested iterator
        iter_path = Path(self.url).resolve() / path.removeprefix(self.sep)
        for item in os.listdir(iter_path):
            item_path = Path(iter_path / item)
            # check if item is directory or an accepted file (.cog, .nc)
            if item_path.is_dir() or self._guess_can_read_subfile(item):
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
        # Make sure that URL is path to a dir
        if os.path.isfile(self.url):
            raise ValueError(f"{self.url} should be path to a directory")
        # Create output folder hierarchy if it doesn't exist (Only on write mode)
        if mode == "w" and not os.path.isdir(self.url):
            os.makedirs(self.url, exist_ok=True)

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        from json import dump

        if self.status != StorageStatus.OPEN:
            raise StoreNotOpenError()
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
            return rioxarray.open_rasterio(file, lock=False, chunks="auto")
        except rasterio.errors.RasterioIOError:
            # try to reopen using netcdf scheme identifier
            # Maybe try use netcdfstore
            return rioxarray.open_rasterio(f"netcdf:{file}:{eov_name}", lock=False, chunks="auto")
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
        return pathlib.Path(file_path).suffix in [".cogs"]

    @staticmethod
    def _guess_can_read_subfile(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".cog", ".nc"]
