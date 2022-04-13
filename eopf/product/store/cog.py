import os
import pathlib
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Optional, Union

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

    Attributes
    ----------
    url: str
        path or url to access
    """

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self._ref: Optional[Any] = None
        self._mode: Optional[str] = None
        self._lock: Optional[Lock] = None

    def __getitem__(self, key: str) -> "EOObject":
        from eopf.product.core.eo_group import EOGroup
        from eopf.product.core.eo_variable import EOVariable

        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if self._mode != "r":
            raise NotImplementedError("Only available in reading mode")

        node = self._select_node(key)
        if isinstance(node, list):
            raise NotImplementedError()

        elif isinstance(node, xarray.Dataset):
            return EOGroup(key, attrs=node.attrs)  # type: ignore[arg-type]
        return EOVariable(key, node)

    def __iter__(self) -> Iterator[str]:
        return self.iter("")

    def __len__(self) -> int:
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if self._mode != "r":
            raise NotImplementedError("Only available in reading mode")
        return len(self._ref)

    def _write_cog(self, value: Any, file_path: str) -> None:
        # write the COG file
        value._data.rio.to_raster(f"{file_path}.cog", tiled=True, lock=self._lock, driver="COG")

    def _write_netCDF4(self, value: "EOObject", file_path: str, var_name: str) -> None:

        # write the netCDF4 file
        nc = EONetCDFStore(f"{file_path}.nc")
        nc.open(mode="w")
        nc[var_name] = value
        nc.close()

    def _is_raster(self, value: Any) -> bool:
        # if the variable has 2 dimensions, it is probably a raster
        if len(value.dims) == 2:
            # if the dimension names are not x,y we need to let
            # rioxarray know which dimension is x and y
            if value.dims[0] != "y" or value.dims[1] != "x":
                value._data.rio.set_spatial_dims(x_dim=value.dims[1], y_dim=value.dims[0], inplace=True)
            return True
        # with S2 L1C image variables have 3 dimesions (band, x, y)
        if len(value.dims) == 3 and (value.dims[0] == "band"):
            return True
        return False

    def _write_eov(self, value: "EOObject", output_dir: str, var_name: str) -> None:

        var_name = var_name.removeprefix("/")
        file_path = os.path.join(output_dir, var_name)
        # determine if variable is a raster, i.e. can be written as cog
        if self._is_raster(value):
            self._write_cog(value, file_path)
        else:
            self._write_netCDF4(value, file_path, var_name)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        import os

        from eopf.product.core import EOGroup, EOVariable

        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if self._mode != "w":
            raise NotImplementedError("Only available in writing mode")

        if isinstance(value, EOVariable):
            self._write_eov(value, self.url, key)
        elif isinstance(value, EOGroup):
            # if the key starts with / the join will not be carried
            if key[0] == os.path.sep:
                key = key[1:]
            output_dir = os.path.join(self.url, key)
            # create the output dir if it does not exist
            if not os.path.isdir(output_dir):
                os.makedirs(output_dir)
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
        if self._mode != "w":
            raise NotImplementedError("Only available in reading mode")
        node = self._select_node(path)
        return isinstance(node, (xarray.Dataset, list))

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        if self._mode != "w":
            raise NotImplementedError("Only available in reading mode")
        node = self._select_node(path)
        return isinstance(node, xarray.DataArray)

    # docstr-coverage: inherited
    @property
    def is_writeable(self) -> bool:
        return False

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        if self._mode != "w":
            raise NotImplementedError("Only available in reading mode")
        node = self._select_node(path)
        if isinstance(node, list):
            raise NotImplementedError()
        if isinstance(node, xarray.Dataset):
            return iter(str(i) for i in iter(node))
        return iter([])

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open(mode=mode)

        self._mode = mode
        self._lock = kwargs.get("lock")
        if mode == "r":
            self._ref = rioxarray.open_rasterio(self.url, **kwargs)
        elif mode == "w":
            self._ref = True
        else:
            raise KeyError("Unsuported mode, only (r)ead or (w)rite")

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self._mode == "r":
            node = self._select_node(group_path)
            if isinstance(node, list):
                raise NotImplementedError()
            node.attrs.update(attrs)  # type: ignore[arg-type]

    # docstr-coverage: inherited
    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return pathlib.Path(file_path).suffix in [".tiff", ".tif", ".jp2"]

    def _select_node(self, path: str) -> Union[xarray.DataArray, xarray.Dataset]:
        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        current, _, sub_path = path.partition(self.sep)
        if path in ["", "/"]:
            return self._ref

        if isinstance(self._ref, list):
            raise NotImplementedError()

        if isinstance(self._ref, xarray.Dataset):
            if path in self._ref:
                return self._ref[path]
            raise KeyError()

        if path == self._ref.name:
            return self._ref

        raise KeyError()
