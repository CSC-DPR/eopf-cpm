import os
import pathlib
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Optional, Union

import fsspec
import rasterio
import rioxarray
import xarray

from eopf.exceptions import StoreNotOpenError
from eopf.product.store.abstract import EOProductStore
from eopf.product.store.netcdf import EONetCDFStore

if TYPE_CHECKING:  # pragma: no cover
    from distributed import Lock

    from eopf.product.core.eo_object import EOObject


class EOCogStore(EOProductStore):
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
    extensions = [".cog", ".nc"]

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self._ref: Optional[Any] = None
        self._mode: Optional[str] = None
        self._lock: Optional[Lock] = None
        self._fs: Optional[Any] = None

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
        from eopf.product.core.eo_group import EOGroup
        from eopf.product.core.eo_variable import EOVariable

        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if self._mode != "r":
            raise NotImplementedError("Only available in reading mode")

        data, attrs = self._select_node(key)
        print(os.listdir(os.path.join(self.url, key)))
        return EOGroup(
            key,
            variables={var_name: EOVariable(name=var_name, data=var_value) for var_name, var_value in data.items()},
            attrs=attrs,  # type: ignore[arg-type]
        )

    def __iter__(self) -> Iterator[str]:
        """Has no functionality within this store"""
        return self.iter("")

    def __len__(self) -> int:
        """Has no functionality within this store"""
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if self._mode != "r":
            raise NotImplementedError("Only available in reading mode")
        return sum(1 for _ in self.iter(""))

    def _write_cog(self, value: Any, file_path: pathlib.Path) -> None:
        """
        This method is used to write rasters to .cog files

        Parameters
        ----------
        value: Any
            rasterIO dataset
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

    def _write_netCDF4(self, value: "EOObject", file_path: str, var_name: str) -> None:
        """
        This method is used to write rasters to .nc (netcdf) files

        Parameters
        ----------
        value: Any
            rasterIO dataset
        """
        # set suffix .nc and transfrom to absolute path
        nc_path = file_path.with_suffix(".nc")
        abs_nc_path = nc_path.resolve()

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

    def _write_eov(self, value: "EOObject", output_dir: str, var_name: str) -> None:
        """
        This metod is used to write an EOVariable to cog or netcdf format.
        Parameters
        ----------
        value: EOVariable
            Variable to be written
        outputdir: str
            path to output folder
        var_name: str
            name of EOVariable
        """
        var_name = var_name.removeprefix(self.sep)
        file_path = output_dir / var_name

        # determine if variable is a raster, i.e. can be written as cog
        if self._is_raster(value):
            self._write_cog(value, file_path)
        else:
            self._write_netCDF4(value, file_path, os.path.basename(var_name))

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

        if self._ref is None:
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
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        super().close()
        self._mode = None
        self._lock = None
        self._ref = None

    # docstr-coverage: inherited
    @property
    def is_erasable(self) -> bool:
        return False

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        if self._mode == "w":
            raise NotImplementedError("Only available in reading mode")
        # Add path separator if missing
        if not path.startswith(self.sep):
            path = self.sep + path
        # Check if path is a directory
        return self._fs.fs.isdir(self.url + path)

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        # check if it is a file or not
        if self._mode == "w":
            raise NotImplementedError("Only available in reading mode")
        # Add path separator if missing
        if not path.startswith(self.sep):
            path = self.sep + path
        # Add extension if missing (TBD: cog or nc)
        if ".nc" not in path:
            path = path + ".nc"
        return self._fs.fs.isfile(self.url + path)

    # docstr-coverage: inherited
    @property
    def is_writeable(self) -> bool:
        return False

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        if self._mode == "w":
            raise NotImplementedError("Only available in reading mode")
        node = self._select_node(path)
        if path in ["", "/"]:
            return os.listdir(self.url)
        if isinstance(node, list):
            raise NotImplementedError()
        if isinstance(node, xarray.Dataset):
            return iter(str(i) for i in iter(node))
        return iter([])

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open(mode=mode)
        self._fs = fsspec.get_mapper(self.url)
        self._mode = mode
        self._lock = kwargs.get("lock")
        if mode == "r":
            self._ref = {}
            self._read_url()
        elif mode == "w":
            self._ref = True
        else:
            raise ValueError("Unsuported mode, only (r)ead or (w)rite")

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        from json import dump

        if self._mode == "r":
            raise NotImplementedError("Only available in writing mode")

        # if there are group attributes write then as json file inside the group
        if attrs:
            init_path = pathlib.Path(group_path).resolve()
            # # the file should be written in the directory representing the group
            attrs_file_path = init_path / self.attributes_file_name
            # remove file if it already exists
            if attrs_file_path.is_file():
                attrs_file_path.unlink()
            # write the json file
            with open(attrs_file_path, "w") as fp:
                dump(attrs, fp)

    # docstr-coverage: inherited
    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".cog", ".nc"]

    def _select_node(self, path: str) -> Union[xarray.DataArray, xarray.Dataset]:
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")

        if path in ["", "/"]:
            return os.listdir(self.url)
            # return self._ref["/"]

        if path in self._ref.keys():
            return self._ref[path]

        raise KeyError()

    def _read_url(self):
        """reading a cog format product"""
        from json import loads
        from pathlib import Path, PosixPath
        from queue import Queue

        # get the absolute path of the given url
        init_path = Path(self.url).resolve()
        if not init_path.exists():
            raise OSError(f"The given path {init_path} does not exist")

        # if a cog or netCDF file is given
        if init_path.is_file():
            eov_name = init_path.stem
            key = "/"
            try:
                var = rioxarray.open_rasterio(init_path)
            except Exception:
                raise TypeError("Can NOT read: {init_path}")
            # ds = xarray.Dataset(data_vars={eov_name:var})
            self._ref[key] = {eov_name: var}, None

        elif init_path.is_dir():
            # if a dir is encoutered we parse through it for files(eov), dirs(eog) and attrs(json)
            # add the url to the queue as init_path
            dir_q: Queue[Path] = Queue()
            dir_q.put(init_path)

            # while there are files/dirs to read continue exploring
            while not dir_q.empty():
                # get an item from the q
                cur_dir = dir_q.get()
                # if a dir is encoutered we explore it for files(eov), dirs(groups) and attrs(json)
                cur_dir_files = []
                for a_path in cur_dir.iterdir():
                    if a_path.is_file() and self.guess_can_read(a_path):
                        # files represent variables and should be added to the current group
                        cur_dir_files.append(a_path)
                    if a_path.is_dir():
                        # subdirs represent other groups which will be put in the q to be read later
                        dir_q.put(a_path)

                # get all variables from current dir
                ds_vars = {}
                for file_path in cur_dir_files:
                    eov_name = file_path.stem
                    try:
                        ds_vars[eov_name] = rioxarray.open_rasterio(file_path)
                    except rasterio.errors.RasterioIOError:
                        # try to reopen using netcdf scheme identifier
                        ds_vars[eov_name] = rioxarray.open_rasterio(f"netcdf:{file_path}:{eov_name}")
                    except Exception as e:
                        raise TypeError(f"Can NOT read: {file_path}", e)

                # determine if the grop has attrs and read them
                attrs_path = cur_dir / self.attributes_file_name
                if attrs_path.is_file():
                    with open(attrs_path, "r") as fp:
                        ds_attrs = loads(fp.read())
                else:
                    ds_attrs = None

                # add the group to the ref
                if cur_dir == init_path:
                    key = "/"
                else:
                    key = str(PosixPath(cur_dir.relative_to(init_path)))

                self._ref[key] = ds_vars, ds_attrs
        else:
            raise NotImplementedError("Only dirs and files can be read")

    def readable(path: str) -> bool:
        # MacOS specific, remove .DS_Store
        path = path.split("/")[-1]
        return not str(path).startswith(".")
