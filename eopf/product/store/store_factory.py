from typing import Any, Optional

from eopf.product.store import EOProductStore
from eopf.product.store.zarr import EOZarrStore


class StoreFactory:
    def __init__(self, default_stores: bool = True) -> None:
        self.item_formats: dict[str, type] = dict()
        self.store_types: set[type] = set()
        if default_stores:
            self.register_store(EOZarrStore)
            # self.register_mapping(EONetcdfStore, "netcdf")

    def register_store(self, store_class: type, *args: str) -> None:
        self.store_types.add(store_class)
        for mapping in args:
            self.item_formats[mapping] = store_class

    def get_store(self, file_path: str, item_format: Optional[str] = None, *args: Any, **kwargs: Any) -> EOProductStore:
        if item_format is not None:
            if item_format in self.item_formats:
                return self.item_formats[item_format](file_path, *args, **kwargs)
            raise KeyError("No registered store with format : " + item_format)
        for store_type in self.store_types:
            if store_type.guess_can_read(file_path):  # type: ignore[attr-defined]
                return store_type(file_path, *args, **kwargs)
        raise KeyError("No registered store compatible with : " + file_path)
