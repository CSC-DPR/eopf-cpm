from typing import TYPE_CHECKING, Any, Iterator, MutableMapping, Optional

from eopf.exceptions import StoreNotOpenError

from ..core import EOGroup
from ..utils import join_eo_path, upsplit_eo_path
from .abstract import EOProductStore, StorageStatus
from .mapping_factory import MappingFactory
from .store_factory import StoreFactory

if TYPE_CHECKING:
    from eopf.product.core.eo_object import EOObject


class SafeHierarchy(EOProductStore):
    """A very simple Store implementation allowing to iterate over a group direct child."""

    def __init__(self) -> None:
        super().__init__("")
        self._child_list: list[str] = []

    def __iter__(self) -> Iterator[str]:
        return iter(self._child_list)

    def is_group(self, path: str) -> bool:
        if path == "" or path == "/":
            return True
        raise NotImplementedError

    def is_variable(self, path: str) -> bool:
        if path == "" or path == "/":
            return False
        raise NotImplementedError

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        pass

    def iter(self, path: str) -> Iterator[str]:
        if path != "":
            raise NotImplementedError
        return iter(self._child_list)

    def __delitem__(self, key: str) -> None:
        raise NotImplementedError

    def __setitem__(self, key: str, value: "EOObject") -> None:
        raise NotImplementedError

    def __getitem__(self, key: str) -> "EOObject":
        raise NotImplementedError

    def __len__(self) -> int:
        return len(self._child_list)

    def _add_child(self, child_name: str) -> None:
        self._child_list.append(child_name)


class EOSafeStore(EOProductStore):
    """Store representation to access to a Safe file on the given URL
    Parameters
    ----------
    url: str
        path url or the target store
    Attributes
    ----------
    url: str
        url to the target store
    sep: str
        file separator
    See Also
    -------
    zarr.storage
    """

    sep = "/"
    SAFE_HIERARCHY_FORMAT = "SafeHierarchy"
    CONFIG_FORMAT = "item_format"
    CONFIG_TARGET = "target_path"
    CONFIG_SOURCE_FILE = "source_path"

    def __init__(
        self,
        url: str,
        store_factory: Optional[StoreFactory] = None,
        mapping_factory: Optional[MappingFactory] = None,
    ) -> None:
        if store_factory is None:
            store_factory = StoreFactory(default_stores=True)
        if mapping_factory is None:
            mapping_factory = MappingFactory(default_mappings=True)
        if url[-1] != "/":
            url = url + "/"
        # FIXME Need to think of a way to manage urlak ospath on windows. Especially with the path in the json.
        super().__init__(url)
        self._store_factory = store_factory
        self._mapping_factory = mapping_factory
        self._accessor_map: dict[str, EOProductStore] = dict()  # map item_format : source path to Store
        self._config_mapping: dict[str, list[dict[str, Any]]] = dict()  # map source path to config read from Json
        self._attrs: dict[str, dict[str, Any]] = dict()

    def _read_json_config(self) -> None:
        json_data = self._mapping_factory.get_mapping(self.url)
        json_config_list = json_data["data_mapping"]
        for config in json_config_list:
            self._add_data_config(config[self.CONFIG_TARGET], config)
        self._attrs = json_data["metadata_mapping"]

    def _contain_hierarchy_config(self, target_path: str) -> bool:
        if target_path not in self._config_mapping:
            return False
        config_list = self._config_mapping[target_path]
        for existing_config in config_list:
            if existing_config[self.CONFIG_FORMAT] == self.SAFE_HIERARCHY_FORMAT:
                return True
        return False

    def _add_data_config(self, target_path: str, config: dict[str, Any]) -> None:
        if target_path in self._config_mapping:
            self._config_mapping[target_path].append(config)
        else:
            self._config_mapping[target_path] = [config]

        if target_path == "":
            return
        source_path_parent, name = upsplit_eo_path(target_path)
        if name is None:
            name = source_path_parent
            source_path_parent = ""

        if not self._contain_hierarchy_config(source_path_parent):
            parent_config = dict()
            parent_config["target_path"] = source_path_parent
            parent_config["source_path"] = source_path_parent
            parent_config["item_format"] = self.SAFE_HIERARCHY_FORMAT
            self._add_data_config(source_path_parent, parent_config)
        safe_hierachy = self._get_accessor(source_path_parent, self.SAFE_HIERARCHY_FORMAT)
        if not isinstance(safe_hierachy, SafeHierarchy):
            raise TypeError("Unexpected accessor type.")
        safe_hierachy._add_child(name)

    def _add_accessor(self, file_path: str, item_format: str) -> EOProductStore:
        mapped_store: EOProductStore
        if item_format == "SafeHierarchy":
            mapped_store = self._accessor_map["SafeHierarchy:" + file_path] = SafeHierarchy()
        else:
            mapped_store = self._store_factory.get_store(file_path, item_format)
        self._accessor_map[item_format + ":" + file_path] = mapped_store
        return mapped_store

    def _get_accessor(self, file_path: str, item_format: str) -> EOProductStore:
        if item_format + ":" + file_path in self._accessor_map:
            return self._accessor_map[item_format + ":" + file_path]
        else:
            return self._add_accessor(file_path, item_format)

    def _get_accessors_from_conf(self, conf_path: str) -> list[(EOProductStore,str)]:
        configs = self._config_mapping[conf_path]
        results = list()
        for conf in configs:
            accessor_file, accessor_local_path = conf[self.CONFIG_SOURCE_FILE].split(':')
            results.append((self._get_accessor(self.url + accessor_file, conf[self.CONFIG_FORMAT]),
                            accessor_local_path))
        return results

    def _split_target_path(self, target_path: str) -> tuple[str, str]:
        safe_target_path = target_path
        local_path: list[str] = []
        while safe_target_path not in self._config_mapping:
            safe_target_path, name = upsplit_eo_path(safe_target_path)
            if safe_target_path is None:
                raise KeyError("Path not found in the configuration")
            local_path.insert(0, name)
        return safe_target_path, join_eo_path(*local_path)

    def _eo_obj_fuse(self, eo_obj_list: list[EOObject]) -> EOObject:
        # Should at least merge dims and attributes of EOVariables/Group.
        if len(eo_obj_list) != 1:
            raise NotImplementedError
        return eo_obj_list[0]

    def _apply_properties(self, eo_obj: EOObject) -> EOObject:
        # Should for example add the dims from the json config to an EOVariable.
        return eo_obj

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        super().open()
        if self._config_mapping is None:
            self._config_mapping = dict()
            self._read_json_config()
        for key in self._accessor_map:
            self._accessor_map[key].open(mode, **kwargs)

    def close(self) -> None:
        super().close()
        for key in self._accessor_map:
            self._accessor_map[key].close()

    def is_group(self, path: str) -> bool:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        safe_path, accessor_path = self._split_target_path(path)
        for accessor, config_accessor_path in self._get_accessors_from_conf(safe_path):
            config_accessor_path = config_accessor_path + accessor_path
            # Stores are not supposed to throw KeyError on is_group
            if accessor.is_group(config_accessor_path):
                return True
        return False

    def is_variable(self, path: str) -> bool:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        safe_path, accessor_path = self._split_target_path(path)
        for accessor, config_accessor_path in self._get_accessors_from_conf(safe_path):
            config_accessor_path = config_accessor_path + accessor_path
            # Stores are not supposed to throw KeyError on is_group
            if accessor.is_variable(config_accessor_path):
                return True
        return False

    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        safe_path, accessor_path = self._split_target_path(group_path)
        for accessor, config_accessor_path in self._get_accessors_from_conf(safe_path):
            config_accessor_path = config_accessor_path + accessor_path
            # We might want to catch Unimplemented/KeyError and throw one if none write_attrs suceed
            accessor.write_attrs(config_accessor_path)

    def iter(self, path: str) -> Iterator[str]:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        safe_path, accessor_path = self._split_target_path(path)

        key_set: set[str] = set()
        for accessor, config_accessor_path in self._get_accessors_from_conf(safe_path):
            config_accessor_path = config_accessor_path + accessor_path
            # Should not throw exception if their store is Open.
            key_set.union(accessor.iter(config_accessor_path))
        return iter(key_set)

    def __getitem__(self, key: str) -> "EOObject":
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")

        if key == "" or key == "/":
            return EOGroup(attrs=self._attrs)

        safe_path, accessor_path = self._split_target_path(key)

        eo_obj_list = list()
        for accessor, config_accessor_path in self._get_accessors_from_conf(safe_path):
            config_accessor_path = config_accessor_path + accessor_path
            # We should catch Key Error, and throw if the object isn't found in any of the accessors
            accessed_object = accessor[config_accessor_path]
            processed_object = self._apply_properties(accessed_object)
            eo_obj_list.append(processed_object)
        return self._eo_obj_fuse(eo_obj_list)

    def __setitem__(self, key: str, value: "EOObject") -> None:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        safe_path, accessor_path = self._split_target_path(key)

        for accessor, config_accessor_path in self._get_accessors_from_conf(safe_path):
            config_accessor_path = config_accessor_path + accessor_path
            # We should catch Key Error, and throw if the object isn't set in any of the accessors
            accessor[config_accessor_path] = value  # I hope we don't need to reverse apply_properties.

    def __delitem__(self, key: str) -> None:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        safe_path, accessor_path = self._split_target_path(key)

        for accessor, config_accessor_path in self._get_accessors_from_conf(safe_path):
            config_accessor_path = config_accessor_path + accessor_path
            # We should catch Key Error, and throw if the object isn't found in any of the accessors
            del accessor[config_accessor_path]

    def __len__(self) -> int:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        count = 0
        for _ in self.iter(""):  # The iter method is non trivial.
            count += 1
        return count

    def __iter__(self) -> Iterator[str]:
        return self.iter("")
