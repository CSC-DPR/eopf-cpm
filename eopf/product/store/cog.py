import os
import pathlib
from collections.abc import MutableMapping
from json import loads
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Optional, Union

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

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self._ref: Any = None
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

        if self._opened is False:
            raise StoreNotOpenError("Store must be open before access to it")
        if self._mode != "r":
            raise NotImplementedError("Only available in reading mode")

        if isinstance(self._ref, EOVariable):
            return self._ref
        else:
            # EOProduct and EOGroup case
            if key in ["", "/"]:
                return self._ref
            else:
                return self._ref[key]

    def __iter__(self) -> Iterator[str]:
        """Has no functionality within this store"""
        return self.iter("")

    def __len__(self) -> int:
        """Has no functionality within this store"""
        if self._opened is False:
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

    def _write_netCDF4(self, value: "EOObject", file_path: pathlib.Path, var_name: str) -> None:
        """
        This method is used to write rasters to .nc (netcdf) files

        Parameters
        ----------
        value: Any
            rasterIO dataset
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
        outputdir: str
            path to output folder
        var_name: str
            name of EOVariable
        """
        var_name = var_name.removeprefix(self.sep)
        file_path: pathlib.Path = output_dir / var_name

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

        if self._opened is False:
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
        if self._opened is False:
            raise StoreNotOpenError("Store must be open before access to it")
        super().close()
        self._mode = None
        self._lock = None
        # self._ref = None
        self._opened = False

    # docstr-coverage: inherited
    @property
    def is_erasable(self) -> bool:
        return False

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        if self._mode == "w":
            raise NotImplementedError("Only available in reading mode")
        if path.startswith("/"):
            path = path.removeprefix("/")
        url_path = Path(self.url).resolve()
        sub_path = url_path / path
        # check if sub_path is a directory
        return sub_path.is_dir()

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        # check if it is a file or not
        if self._mode == "w":
            raise NotImplementedError("Only available in reading mode")
        if path.startswith("/"):
            path = path.removeprefix("/")
        url_path = Path(self.url).resolve()
        sub_path = url_path / path
        # check if sub_path is a directory
        return sub_path.is_file()

    # docstr-coverage: inherited
    @property
    def is_writeable(self) -> bool:
        return False

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:

        if self._mode == "w":
            raise NotImplementedError("Only available in reading mode")
        if path in ["", "/"]:
            return self._ref.__iter__()
        return self._ref[path].__iter__()

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open(mode=mode)
        self._mode = mode

        self._lock = kwargs.get("lock")
        if mode == "r":
            self._opened = True
            self._read_url()
        elif mode == "w":
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
        # return pathlib.Path(file_path).suffix in [".cog", ".nc"]
        # TBD put the above line after correcting nc reading
        return pathlib.Path(file_path).suffix in [".cog", ".nc"]

    def _select_node(self, path: str) -> Union[xarray.DataArray, xarray.Dataset]:
        if self._opened is False:
            raise StoreNotOpenError("Store must be open before access to it")
        raise NotImplementedError()

    def _read_eov(self, file: Path, eov_name: str) -> Any:
        # TBD to add description
        # TBD there is still an iessue with row_time.nc in coordinates/image_grid

        try:
            return rioxarray.open_rasterio(file)
        except rasterio.errors.RasterioIOError:
            # try to reopen using netcdf scheme identifier
            return rioxarray.open_rasterio(f"netcdf:{file}:{eov_name}")
        except Exception as e:
            # this should be another error type
            raise TypeError(f"Can NOT read: {file}", e)

    def _read_url(self) -> None:
        """reading a cog format product"""
        # TBD to add description

        from eopf.product.core import EOVariable

        # get the absolute path of the given url
        url_path = Path(self.url).resolve()
        if not url_path.exists():
            raise OSError(f"The given path {url_path} does not exist")

        # if a cog or netCDF file is given
        if url_path.is_file():
            eov_name = url_path.stem
            eov = self._read_eov(url_path, eov_name)
            self._ref = EOVariable(eov_name, eov)
        elif url_path.is_dir():
            # if a dir is encoutered we parse through it for files(eov), dirs(eog) and attrs(json)
            self._ref = self._read_dir(url_path)
        else:
            raise NotImplementedError("Only dirs and files can be read")

    def _read_attrs(self, dir_path: Path) -> Any:
        # TBD to add description
        # TBD add try except for reading
        attrs_path = dir_path / self.attributes_file_name
        if attrs_path.is_file():
            with open(attrs_path, "r") as fp:
                return loads(fp.read())
        else:
            return None

    def _read_dir(self, dir_path: Path, parent: Any = None) -> Any:
        # TBD to add description
        from eopf.product.core import EOGroup, EOProduct

        # list of files from which to extract EOV
        dir_files: list[Path] = []
        # list of subdirs from which to sub EOG
        subdirs: list[Path] = []

        # explore current dir and populate the list of files and subdirs
        for a_path in dir_path.iterdir():
            if a_path.is_file() and self.guess_can_read(str(a_path)):
                # only files that cna be read by this store are added
                dir_files.append(a_path)
            if a_path.is_dir():
                subdirs.append(a_path)

        # get all variables from current dir
        vars = {}
        for file in dir_files:
            eov_name = file.stem
            vars[eov_name] = self._read_eov(file, eov_name)

        # determine if the group/eop has attrs and read them
        attrs = self._read_attrs(dir_path)

        if parent is None:
            # this the root eop/eog
            measurements_path = dir_path / "measurements"
            coordinates_path = dir_path / "coordinates"
            # if measurements and coordinates are present, then an entire EOProduct
            # is requested to be loaded
            if measurements_path.exists() and coordinates_path.exists():
                eop = EOProduct(name=dir_path.name, attrs=attrs)
                for dir in subdirs:
                    self._read_dir(dir, eop)
                return eop
            else:
                # in case part of the products are loaded
                # here eop is needed just to avoid missing EOProduct errors
                eop = EOProduct(name=dir_path.name)
                root_eog: EOGroup = eop.add_group(name=dir_path.name, attrs=attrs)
                for name, value in vars.items():
                    root_eog._add_local_variable(name, value)
                for dir in subdirs:
                    self._read_dir(dir, root_eog)
                return root_eog
        else:
            # this is not the root eog/eop
            cur_eog = parent.add_group(name=dir_path.name, attrs=attrs)
            for name, value in vars.items():
                cur_eog._add_local_variable(name, value)
            for dir in subdirs:
                self._read_dir(dir, cur_eog)
