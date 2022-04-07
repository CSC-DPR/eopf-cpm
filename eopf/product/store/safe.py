import os
import warnings
from functools import reduce
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterator,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
)

import dask.array as da
import numpy as np

from eopf.exceptions import StoreNotOpenError

from ..utils import join_eo_path_optional, partition_eo_path, upsplit_eo_path
from .abstract import EOProductStore, StorageStatus
from .mapping_factory import EOMappingFactory
from .store_factory import EOStoreFactory

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_object import EOObject


class SafeHierarchy(EOProductStore):
    """A very simple Store implementation allowing to iterate over a group direct child."""

    # docstr-coverage: inherited
    def __init__(self) -> None:
        super().__init__("")
        self._child_list: list[str] = []

    def __iter__(self) -> Iterator[str]:
        return iter(self._child_list)

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        if path in ["", "/"]:
            return True
        raise NotImplementedError()

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        if path in ["", "/"]:
            return False
        raise NotImplementedError()

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        pass

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        if path != "":
            raise NotImplementedError()
        return iter(self._child_list)

    def __delitem__(self, key: str) -> None:
        raise NotImplementedError()

    def __setitem__(self, key: str, value: "EOObject") -> None:
        from ..core import EOGroup

        cond1 = not isinstance(value, EOGroup)
        cond2 = key != "" and value.name not in self._child_list
        if cond1 or cond2:
            raise KeyError("Safe can't write key outside of it's dictionary")

    def __getitem__(self, key: str) -> "EOObject":
        from ..core import EOGroup

        if key in ["", "/"]:
            return EOGroup()
        raise NotImplementedError()

    def __len__(self) -> int:
        return len(self._child_list)

    def _add_child(self, child_name: str) -> None:
        if not child_name:
            raise KeyError("Invalid store group name")
        self._child_list.append(child_name)


# Safe store parameters transformations:


def _transformation_dimensions(eo_obj: "EOObject", parameter: Any) -> "EOObject":
    """Replace the object dimension with the list of dimensions parameter"""
    eo_obj.assign_dims(parameter)
    return eo_obj


def _transformation_attributes(eo_obj: "EOObject", parameter: Any) -> "EOObject":
    """Update the object attributes with the dictionary of attribute parameter"""
    eo_obj.attrs.update(parameter)
    return eo_obj


def _transformation_sub_array(eo_obj: "EOObject", parameter: Any) -> "EOObject":
    """Index the array according to the parameter. If the parameter is a single index, the dimension is removed."""
    from ..core import EOVariable

    if not isinstance(eo_obj, EOVariable):
        raise TypeError()
    return eo_obj.isel(parameter)


def _transformation_pack_bits(eo_obj: "EOObject", parameter: Any) -> "EOObject":
    """Pack bit the parmater dimension of eo_obj."""
    from ..core import EOVariable

    if not isinstance(eo_obj, EOVariable):
        raise TypeError()
    attrs = eo_obj.attrs
    dims = list(eo_obj.dims)
    data = da.map_blocks(np.packbits, eo_obj._data, axis=dims.index(parameter), bitorder="little")
    dims.remove(parameter)
    return EOVariable(data=data, dims=tuple(dims), attrs=attrs)


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
    DEFAULT_PARAMETERS_TRANSFORMATIONS_LIST: list[tuple[str, Callable[["EOObject", Any], "EOObject"]]] = [
        ("attributes", _transformation_attributes),
        ("sub_array", _transformation_sub_array),
        ("pack_bits", _transformation_pack_bits),
        ("dimensions", _transformation_dimensions),  # dimensions should be after dimension dependant tranfo
    ]

    # docstr-coverage: inherited
    def __init__(
        self,
        url: str,
        store_factory: Optional[EOStoreFactory] = None,
        mapping_factory: Optional[EOMappingFactory] = None,
        parameters_transformations: Optional[list[tuple[str, Callable[["EOObject", Any], "EOObject"]]]] = None,
    ) -> None:
        if store_factory is None:
            store_factory = EOStoreFactory(default_stores=True)
        if mapping_factory is None:
            mapping_factory = EOMappingFactory(default_mappings=True)
        if parameters_transformations is None:
            self._parameters_transformations = self.DEFAULT_PARAMETERS_TRANSFORMATIONS_LIST
        else:
            self._parameters_transformations = parameters_transformations
        if url[-1] != "/":
            url = url + "/"
        # FIXME Need to think of a way to manage urlak ospath on windows. Especially with the path in the json.
        super().__init__(url)
        self._accessor_manager = SafeMappingManager(url, store_factory, mapping_factory)

    def __delitem__(self, key: str) -> None:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        for safe_path, accessor_path in self._accessor_manager.split_target_path(key):
            mapping_match_list = self._accessor_manager.get_accessors_from_mapping(safe_path)
            for accessor, config_accessor_path, _ in mapping_match_list:
                config_accessor_path = join_eo_path_optional(config_accessor_path, accessor_path)
                # We should catch Key Error, and throw if the object isn't found in any of the accessors
                del accessor[config_accessor_path]

    def __getitem__(self, key: str) -> "EOObject":
        from ..core import EOGroup

        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")

        eo_obj_list: list[EOObject] = list()
        for safe_path, accessor_path in self._accessor_manager.split_target_path(key):
            if key in ["", "/"]:
                eo_obj_list.append(EOGroup())
            mapping_match_list = self._accessor_manager.get_accessors_from_mapping(safe_path)
            for accessor, config_accessor_path, config in mapping_match_list:
                config_accessor_path = join_eo_path_optional(config_accessor_path, accessor_path)
                # We should catch Key Error, and throw if the object isn't found in any of the accessors
                accessed_object = accessor[config_accessor_path]
                processed_object = self._apply_mapping_properties(accessed_object, config)
                eo_obj_list.append(processed_object)
        return self._eo_object_merge(*eo_obj_list)

    def __iter__(self) -> Iterator[str]:
        return self.iter("")

    def __len__(self) -> int:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        count = 0
        for _ in self.iter(""):  # The iter method is non trivial.
            count += 1
        return count

    def __setitem__(self, key: str, value: "EOObject") -> None:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        for safe_path, accessor_path in self._accessor_manager.split_target_path(key):
            mapping_match_list = self._accessor_manager.get_accessors_from_mapping(safe_path)
            for accessor, config_accessor_path, _ in mapping_match_list:
                config_accessor_path = join_eo_path_optional(config_accessor_path, accessor_path)
                # We should catch Key Error, and throw if the object isn't set in any of the accessors
                accessor[config_accessor_path] = value  # I hope we don't need to reverse apply_properties.

    # docstr-coverage: inherited
    def close(self) -> None:
        super().close()
        self._accessor_manager.close_all()

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        for safe_path, accessor_path in self._accessor_manager.split_target_path(path):
            mapping_match_list = self._accessor_manager.get_accessors_from_mapping(safe_path)
            for accessor, config_accessor_path, _ in mapping_match_list:
                config_accessor_path = join_eo_path_optional(config_accessor_path, accessor_path)
                # Stores are not supposed to throw KeyError on is_group
                if accessor.is_group(config_accessor_path):
                    return True
        return False

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        for safe_path, accessor_path in self._accessor_manager.split_target_path(path):
            mapping_match_list = self._accessor_manager.get_accessors_from_mapping(safe_path)
            for accessor, config_accessor_path, _ in mapping_match_list:
                config_accessor_path = join_eo_path_optional(config_accessor_path, accessor_path)
                # Stores are not supposed to throw KeyError on is_group
                if accessor.is_variable(config_accessor_path):
                    return True
        return False

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        key_set: set[str] = set()
        for safe_path, accessor_path in self._accessor_manager.split_target_path(path):
            mapping_match_list = self._accessor_manager.get_accessors_from_mapping(safe_path)
            for accessor, config_accessor_path, _ in mapping_match_list:
                config_accessor_path = join_eo_path_optional(config_accessor_path, accessor_path)
                # Should not throw exception if their store is Open.
                key_set = key_set.union(accessor.iter(config_accessor_path))
        return iter(key_set)

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:
        # Must not read the product mapping between  super.open and accessor.open
        # Otherwise Hierachy accessor are opened twice.
        super().open()
        self._accessor_manager.open_all(mode, **kwargs)
        if mode == "w":
            try:
                os.mkdir(os.path.expanduser(self.url))
            except FileExistsError:
                ...

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        if self.status is StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open before access to it")
        for safe_path, accessor_path in self._accessor_manager.split_target_path(group_path):
            mapping_match_list = self._accessor_manager.get_accessors_from_mapping(safe_path)
            for accessor, config_accessor_path, _ in mapping_match_list:
                config_accessor_path = join_eo_path_optional(config_accessor_path, accessor_path)
                # We might want to catch Unimplemented/KeyError and throw one if none write_attrs suceed
                accessor.write_attrs(config_accessor_path)

    def _apply_mapping_properties(self, eo_obj: "EOObject", config: dict[str, Any]) -> "EOObject":
        """Modify the eo_object according to the json data_mapping config.

        Parameters
        ----------
        eo_obj: EOObject
            object to modify
        config: dict
            configuration to apply

        Returns
        -------
        EOObject
        """
        # Should for example add the dims from the json config to an EOVariable.
        if "parameters" not in config:
            return eo_obj
        parameters = config["parameters"]
        for parameter_name, parameter_transformation in self._parameters_transformations:
            if parameter_name in parameters:
                eo_obj = parameter_transformation(eo_obj, parameters["parameter_name"])
        # add a warning if a parameter is missing from _parameters_transformations ?
        return eo_obj

        from ..core import EOGroup, EOVariable

        data = eo_obj._data if isinstance(eo_obj, EOVariable) else None
        attrs = eo_obj.attrs
        dims = eo_obj.dims

        if "dimensions" in parameters:
            dims = parameters["dimensions"]
        else:
            dims = eo_obj.dims

        if isinstance(eo_obj, EOVariable):
            return EOVariable(data=data, dims=dims, attrs=attrs)
        return EOGroup(dims=dims, attrs=attrs)

    def _eo_object_merge(self, *eo_obj_list: "EOObject") -> "EOObject":
        """Merge all eo objectect passed to this function.
        We do an union on dims and attributes.

        Parameters
        ----------
        eo_obj_list

        Returns
        -------
        EOObject
        """
        # Should at least merge dims and attributes of EOVariables/Group.
        from ..core import EOGroup

        if not eo_obj_list:
            raise KeyError("Empty object match.")
        if len(eo_obj_list) == 1:
            return eo_obj_list[0]
        dims: set[str] = set()
        attrs = dict()
        for eo_obj in eo_obj_list:
            if not isinstance(eo_obj, EOGroup):
                raise NotImplementedError
            dims = dims.union(eo_obj.dims)
            attrs.update(eo_obj.attrs)
        return EOGroup(attrs=attrs, dims=tuple(dims))


class SafeMappingManager:
    """Class managing reading the Safe store configuration and creating as needed the associated accessors."""

    CONFIG_FORMAT = "item_format"
    CONFIG_TARGET = "target_path"
    CONFIG_SOURCE_FILE = "source_path"
    CONFIG_ACCESSOR_CONF_DEC = "accessor_config"

    SAFE_HIERARCHY_FORMAT = "SafeHierarchy"
    CONFIG_ACCESSOR_CONFIG = "accessor_conf_exp"
    CONFIG_ACCESSOR_ID = "accessor_id"

    def __init__(self, url: str, store_factory: EOStoreFactory, mapping_factory: EOMappingFactory) -> None:
        # FIXME Need to think of a way to manage urlak ospath on windows. Especially with the path in the json.
        self._url = url
        self._store_factory = store_factory
        self._mapping_factory = mapping_factory
        # _accesor_map map the open accessor by file and accessor type and by config id
        # to avoid reopening them in case of reuse.
        # The value is the product store and it's config (to allow reopening)
        self._accessor_map: dict[str, dict[Any, tuple[Optional[EOProductStore], dict[str, Any]]]] = dict()
        # map item_format : source path to Store
        # _config_mapping contain the mapping config by target_path read from the json mapping.
        # It's a dictionary of list as we can have multiple mapping for the same target_path.
        self._config_mapping: dict[str, list[dict[str, Any]]] = dict()  # map source path to config read from Json
        self._mode = "CLOSED"
        self._open_kwargs: dict[str, Any] = dict()

    def __iter__(self) -> Iterator[tuple[EOProductStore, dict[str, Any]]]:
        for accessor_map_2 in self._accessor_map.values():
            for accessor, config in accessor_map_2.values():
                if accessor:
                    yield accessor, config

    def close_all(self) -> None:
        """Close all managed accessors and switch default mode to closed."""
        self._mode = "CLOSED"
        for accessor, _accessor_config in self:
            accessor.close()

    def get_accessors_from_mapping(
        self,
        conf_path: str,
    ) -> Sequence[Tuple[EOProductStore, Optional[Any], dict[str, Any]]]:
        """Get all accessor corresponding to the configs of conf_path.
        As multiple mapping car match a single conf_path, it can return multiple accessors.
        """
        configs = self._config_mapping[conf_path]
        results = list()
        for conf in configs:
            accessor_source_split = conf[self.CONFIG_SOURCE_FILE].split(":")
            if len(accessor_source_split) > 2:
                raise ValueError(f"Invalid {self.CONFIG_SOURCE_FILE} : {conf[self.CONFIG_SOURCE_FILE]}")
            accessor_file = accessor_source_split[0]
            accessor_local_path = None
            if len(accessor_source_split) == 2:
                accessor_local_path = accessor_source_split[1]
            elif "parameters" in conf and "xpath" in conf["parameters"]:
                accessor_local_path = conf["parameters"]["xpath"]
            accessor = self._get_accessor(
                accessor_file,
                conf[self.CONFIG_FORMAT],
                conf[self.CONFIG_ACCESSOR_ID],
                conf[self.CONFIG_ACCESSOR_CONFIG],
            )
            if accessor is not None:  # We also want to append accessor of len 0.
                results.append((accessor, accessor_local_path, conf))
        return results

    def open_all(self, mode: str = "r", **kwargs: Any) -> None:
        """Open all managed accessors and switch default mode to opened.
        On first opening read the json config file.
        """
        if not self._config_mapping:
            self._read_product_mapping()

        self._open_kwargs = kwargs
        self._mode = mode
        for accessor, _accessor_config in self:
            # FIXME Should probably check if the accessor is open intead of seting it to None
            # FIXME when opeing fail, otherwise we can't reopen it later with another mode.
            accessor.open(mode, **_accessor_config, **kwargs)

    def split_target_path(self, target_path: str) -> Sequence[tuple[str, Optional[str]]]:
        """Split target_path between a path where a mapping is registered, and a local path."""
        if target_path and target_path[0] == "/":
            safe_target_path = target_path[1:]
        else:
            safe_target_path = target_path
        local_path: list[str] = []
        return_list = list()
        while True:
            # Both can be true we then need to return both
            if safe_target_path in self._config_mapping:
                return_list.append((safe_target_path, join_eo_path_optional(*local_path)))
            if "/" + safe_target_path in self._config_mapping:
                return_list.append(("/" + safe_target_path, join_eo_path_optional(*local_path)))
            if return_list:
                return return_list
            safe_target_path, name = upsplit_eo_path(safe_target_path)
            if name == "":
                raise KeyError("Path not found in the configuration")
            local_path.insert(0, name)

    def _add_accessor(
        self,
        file_path: str,
        item_format: str,
        accessor_config_id: Any,
        accessor_config: dict[str, Any],
    ) -> Optional[EOProductStore]:
        """Add an accessor (sub store) to the opened accessor dictionary.
        The accessor is created using the _store_factory (except for SafeHierarchy).
        """

        mapped_store: Optional[EOProductStore]
        accessor_id = item_format + ":" + file_path
        if accessor_id not in self._accessor_map:
            self._accessor_map[accessor_id] = dict()
        if item_format == "SafeHierarchy":
            mapped_store = SafeHierarchy()
        else:
            mapped_store = self._store_factory.get_store(
                self._url + file_path,
                item_format,
            )
        try:
            mapped_store.open(mode=self._mode, **accessor_config, **self._open_kwargs)
        except NotImplementedError:
            mapped_store = None
            warnings.warn("Unimplemented store mode")
        self._accessor_map[accessor_id][accessor_config_id] = (mapped_store, accessor_config)
        return mapped_store

    def _add_data_mapping(self, target_path: str, config: dict[str, Any], json_data: dict[str, Any]) -> None:
        """Add a mapping from the format read from json to our internal format.
        Also add own parents mapping and register new children to ancestors hierarchy store.
        """
        self._extract_accessor_config(config, json_data)
        if target_path in self._config_mapping:
            self._config_mapping[target_path].append(config)
        else:
            self._config_mapping[target_path] = [config]

        if target_path in ["", "/"]:
            return
        source_path_parent, name = upsplit_eo_path(target_path)

        if not self._contain_hierarchy_mapping(source_path_parent):
            parent_config = dict()
            parent_config["target_path"] = source_path_parent
            parent_config["source_path"] = source_path_parent
            parent_config["item_format"] = self.SAFE_HIERARCHY_FORMAT
            self._add_data_mapping(source_path_parent, parent_config, json_data)
        safe_hierachy = self._get_accessor(source_path_parent, self.SAFE_HIERARCHY_FORMAT, frozenset(), dict())
        if not isinstance(safe_hierachy, SafeHierarchy):
            raise TypeError("Unexpected accessor type.")
        safe_hierachy._add_child(name)

    def _contain_hierarchy_mapping(self, target_path: str) -> bool:
        """Check if a hierarchy mapping is defined for target_path."""
        if target_path not in self._config_mapping:
            return False
        config_list = self._config_mapping[target_path]
        for existing_config in config_list:
            if existing_config[self.CONFIG_FORMAT] == self.SAFE_HIERARCHY_FORMAT:
                return True
        return False

    def _extract_accessor_config(
        self,
        config: dict[str, Any],
        config_definitions: dict[str, Any],
    ) -> None:
        """Extract in config the accessor config by matching it to config_definitions and build an hashable id of it."""
        if self.CONFIG_ACCESSOR_CONF_DEC in config:
            config_declarations = config[self.CONFIG_ACCESSOR_CONF_DEC]
        else:
            config_declarations = dict()
        # The reduce allow us to do a get item on a nested directory using a split path.
        accessor_config = {
            config_key: reduce(dict.get, partition_eo_path(config_path), config_definitions)  # type: ignore[arg-type]
            for config_key, config_path in config_declarations.items()
        }
        config[self.CONFIG_ACCESSOR_ID] = frozenset(config_declarations.items())
        config[self.CONFIG_ACCESSOR_CONFIG] = accessor_config

    def _get_accessor(
        self,
        file_path: str,
        item_format: str,
        accessor_config_id: Any,
        accessor_config: dict[str, Any],
    ) -> Optional[EOProductStore]:
        """Get an accessor from the opened accessors dictionary. If it's not present a new one is added."""
        accessor_id = item_format + ":" + file_path
        if accessor_id in self._accessor_map and accessor_config_id in self._accessor_map[accessor_id]:
            return self._accessor_map[accessor_id][accessor_config_id][0]
        else:
            return self._add_accessor(file_path, item_format, accessor_config_id, accessor_config)

    def _read_product_mapping(self) -> None:
        """Read mapping from the mapping factory and fill _config_mapping from it."""
        json_data = self._mapping_factory.get_mapping(self._url)
        json_config_list = json_data["data_mapping"]
        for config in json_config_list:
            self._add_data_mapping(config[self.CONFIG_TARGET], config, json_data)
