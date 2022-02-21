from typing import Optional

from eopf.product.store import EOProductStore
from eopf.product.store.zarr import EOZarrStore


class StoreMappingFactory:
    def __init__(self, default_mappings=True):
        self.item_format_mappings = dict()
        self.store_types = set()
        if default_mappings:
            self.register_mapping(EOZarrStore)
            # self.register_mapping(EONetcdfStore, "netcdf")

    def register_mapping(self, store_class: type, *args: str) -> None:
        self.store_types.add(store_class)
        for mapping in args:
            self.item_format_mappings[mapping] = store_class

    def get_mapping(self, file_path: str, item_format: Optional[str] = None, *args, **kwargs) -> EOProductStore:
        if item_format is not None:
            if item_format in self.item_format_mappings:
                return self.item_format_mappings[item_format](file_path, *args, **kwargs)
            raise KeyError("No registered store with format : " + item_format)
        for store_type in self.store_types:
            if store_type.guess_can_read(file_path):
                return store_type(file_path, *args, **kwargs)
        raise KeyError("No registered store compatible with : " + file_path)
