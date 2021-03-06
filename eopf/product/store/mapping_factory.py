import json
import re
from pathlib import Path
from typing import Any

import pkg_resources

from eopf.conf import conf_loader


class EOMappingFactory:
    FILENAME_RECO = "filename_pattern"
    TYPE_RECO = "product_type"
    RECO = "recognition"

    def __init__(self, default_mappings: bool = True) -> None:
        self.mapping_set: set[str] = set()
        if default_mappings:
            self.load_default_mapping()

    def load_default_mapping(self) -> None:
        conf = conf_loader()
        path_directory = Path(conf.mapping)
        for mapping_path in path_directory.glob("*.json"):
            self.register_mapping(str(mapping_path))

        for resource in pkg_resources.iter_entry_points("eopf.store.mapping_folder"):
            module, _, folder = resource.module_name.rpartition(".")
            path_directory = Path(pkg_resources.resource_filename(module, folder))
            for mapping_path in path_directory.glob("*.json"):
                self.register_mapping(str(mapping_path))

    def get_mapping(self, file_path: str = "", product_type: str = "") -> dict[str, Any]:
        if file_path:
            recognised = file_path
            reco = self.FILENAME_RECO
        elif product_type:
            recognised = product_type
            reco = self.TYPE_RECO
        else:
            raise ValueError("Must provide either file_path or product_type.")

        for json_mapping_path in self.mapping_set:
            with open(json_mapping_path) as json_mapping_file:
                json_mapping_data = json.load(json_mapping_file)
                if self.guess_can_read(json_mapping_data, recognised, reco):
                    return json_mapping_data
        raise KeyError(f"No registered mapping compatible with : {recognised}")

    def guess_can_read(self, json_mapping_data: dict[str, Any], recognised: str, recogniton_key: str) -> bool:
        pattern = json_mapping_data.get("recognition", {}).get(recogniton_key)
        if pattern:
            return re.match(pattern, recognised) is not None
        return False

    def register_mapping(self, store_class: str) -> None:
        self.mapping_set.add(store_class)
