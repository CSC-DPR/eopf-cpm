import json
from typing import Any


class MappingFactory:
    def __init__(self, default_mappings=True):
        self.mapping_set = set()
        if default_mappings:
            self.register_mapping("S3_OL_1_EFR_mapping.json")

    def register_mapping(self, store_class: str) -> None:
        self.mapping_set.add(store_class)

    def get_mapping(self, file_path: str) -> dict[str, Any]:
        for json_mapping_file in self.mapping_set:
            json_mapping_data = json.load(json_mapping_file)
            if self.guess_can_read(json_mapping_data, file_path):
                return json_mapping_data
        raise KeyError("No registered store compatible with : " + file_path)

    def guess_can_read(self, json_mapping_data: dict[str, Any], file_path: str):
        return True
