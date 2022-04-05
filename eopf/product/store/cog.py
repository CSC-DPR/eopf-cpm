from email.mime import base
from lib2to3.pgen2 import driver
import pathlib
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator, Optional, Union
from eopf.product.core.eo_group import EOGroup
from eopf.product.store.netcdf import EONetCDFStore

import rioxarray
import xarray

from eopf.exceptions import StoreNotOpenError
from eopf.product.store.abstract import EOProductStore

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


    def _write_cog(self, value: "EOObject", file_path: str) -> None:
        file_path += ".cog"
        value._data.rio.to_raster(
            file_path,
            tiled=True,
            lock=self._lock,
            driver="COG"
        )

    def _write_netCDF4(self, value: "EOObject", file_path: str, var_name: str) -> None:
        file_path += ".nc"
        nc = EONetCDFStore(file_path)
        nc.open(mode="w")
        nc[var_name] = value
        nc.close()

    def _write_eov(self, value: "EOObject", output_dir: str, var_name: str):
        from os.path import join

        file_path = join(output_dir, var_name)
        if len(value.dims) == 3 and (value.dims[0]=='band'):
            self._write_cog(value, file_path)
        else:
            self._write_netCDF4(value, file_path, var_name)  

    def __setitem__(self, key: str, value: "EOObject") -> None:
        from eopf.product.core.eo_variable import EOVariable
        import os

        if self._ref is None:
            raise StoreNotOpenError("Store must be open before access to it")
        if self._mode != "w":
            raise NotImplementedError("Only available in writing mode")
            
        if isinstance(value, EOVariable):
            self._write_eov(value, self.url, key)
        elif isinstance(value, EOGroup):
            output_dir = os.path.join(self.url, key)
            os.mkdir(output_dir, mode=777)
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
        if self._mode != "w":
            raise NotImplementedError("Only available in reading mode")
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
