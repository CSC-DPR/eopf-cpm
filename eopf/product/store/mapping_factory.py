import glob
import importlib.resources
import json
import re
from pathlib import Path
from typing import Any

import fsspec



class MappingFactory:
    def __init__(self, default_mappings: bool = True) -> None:
        self.mapping_set: set[str] = set()
        if default_mappings:
            path_directory = Path(__file__).parent / "mapping"
            for mapping_path in path_directory.glob("*.json"):
                self.register_mapping(str(mapping_path))

    def register_mapping(self, store_class: str) -> None:
        self.mapping_set.add(store_class)

    def get_mapping(self, file_path: str) -> dict[str, Any]:
        for json_mapping_path in self.mapping_set:
            with open(json_mapping_path) as json_mapping_file:
                json_mapping_data = json.load(json_mapping_file)
                if self.guess_can_read(json_mapping_data, file_path):
                    return json_mapping_data
        raise KeyError("No registered store compatible with : " + file_path)

    def guess_can_read(self, json_mapping_data: dict[str, Any], file_path: str) -> bool:
        fsmap = fsspec.get_mapper(file_path)
        *_, dir_name = fsmap.root.rpartition(fsmap.fs.sep)
        pattern = json_mapping_data.get("recognition", {}).get("filename_pattern")
        if pattern:
            return re.match(pattern, dir_name) is not None
        return False
