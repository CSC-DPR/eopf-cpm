from typing import Any, Optional

from eopf.product.store import EOProductStore
from eopf.product.store.rasterio import EORasterIOAccessor


class EOStoreFactory:
    def __init__(self, default_stores: bool = True) -> None:
        self.item_formats: dict[str, type[EOProductStore]] = dict()
        self.store_types: set[type[EOProductStore]] = set()
        if default_stores:
            from eopf.product.store.grib import EOGribAccessor
            from eopf.product.store.manifest import ManifestStore
            from eopf.product.store.netcdf import (
                EONetCDFStore,
                EONetcdfStringToTimeAccessor,
            )
            from eopf.product.store.wrappers import FromAttributesToVariableAccessor
            from eopf.product.store.xml_accessors import (
                XMLAnglesAccessor,
                XMLTPAccessor,
            )
            from eopf.product.store.zarr import EOZarrStore

            self.register_store(EOZarrStore)
            self.register_store(EONetCDFStore, "netcdf")
            self.register_store(EONetcdfStringToTimeAccessor, "netcdf_string_to_time")
            self.register_store(ManifestStore, "xmlmetadata")
            self.register_store(EOGribAccessor, "grib")
            self.register_store(EORasterIOAccessor, "jp2")
            self.register_store(XMLAnglesAccessor, "xmlangles")
            self.register_store(XMLTPAccessor, "xmltp")
            self.register_store(FromAttributesToVariableAccessor, "attribute_element_to_float_variable")

    def register_store(self, store_class: type[EOProductStore], *args: str) -> None:
        self.store_types.add(store_class)
        for mapping in args:
            self.item_formats[mapping] = store_class

    def get_store(self, file_path: str, item_format: Optional[str] = None, *args: Any, **kwargs: Any) -> EOProductStore:
        if item_format is not None:
            if item_format in self.item_formats:
                return self.item_formats[item_format](file_path, *args, **kwargs)
            raise KeyError(f"No registered store with format : {item_format}")
        for store_type in self.store_types:
            if store_type.guess_can_read(file_path):
                return store_type(file_path, *args, **kwargs)
        raise KeyError(f"No registered store compatible with : {file_path}")
