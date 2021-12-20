"""
Define Store Objects for EOProduct.
"""

import os
import pathlib
import re
from abc import abstractclassmethod, abstractmethod
from typing import Iterable, Type

import xarray as xr
import zarr
from fsspec.spec import AbstractFileSystem

from .core import EOGroup, EOProduct, EOVariable


class EOProductStore(AbstractFileSystem):
    @abstractclassmethod
    def dump(cls, eoproduct: EOProduct) -> "EOProductStore":
        """Generate a EOProductStore from the given EOProduct"""

    @abstractmethod
    def load(self, klass: Type[EOProduct]) -> EOProduct:
        """Generate a EOProduct f the given EOProduct class with
        the self EOProductStore representation
        """

    @abstractmethod
    def update(self, product: EOProduct) -> None:
        """ """

    @abstractmethod
    def write(self, product: EOProduct) -> None:
        """ """

    @abstractmethod
    def read(self, mask_and_scale: bool = False) -> EOProduct:
        """ """

    @abstractclassmethod
    def open(self, path, mode="rw", block_size=None, cache_options=None, compression=None, **kwargs):
        """ """

    @abstractmethod
    def __enter__(self):
        """ """

    @abstractmethod
    def __exit__(self, *args):
        """ """


def find_files_paths(abs_path, extension):
    yield from (
        pathlib.Path(os.path.join(abs_path, filename))
        for filename in os.listdir(abs_path)
        if os.path.splitext(filename)[-1] == extension
    )


class NetCDFReader(EOProductStore):
    EXTENSION: str = ".nc"
    _path: str

    def __init__(self, path, groups_patterns=tuple(), name=""):
        self._path: str = path
        self._groups_patterns: Iterable[Iterable[str, str]] = groups_patterns
        self._name: str = name or pathlib.Path(path).parent.name

    def _define_group_on_file(self, groups={}):
        for file in find_files_paths(self._path, extension=self.EXTENSION):
            for pattern, group_name in self._groups_patterns:
                if re.match(pattern, file.name):
                    groups.setdefault(group_name, []).append(file)
                    break
            else:
                groups[file.name.replace(self.EXTENSION, "")] = file

    def read(self) -> EOProduct:

        groups_on_data = {}
        self._define_group_on_file(groups=groups_on_data)
        eogroups = {}
        for group_name, data in groups_on_data.items():
            if isinstance(data, pathlib.Path):
                variables = xr.open_dataset(data, mask_and_scale=False, chunks={}).values()
            else:
                variables = []
                subgroups = []
                for file in data:
                    try:
                        variables.append(xr.open_dataarray(file))
                    except ValueError:
                        subgroups.append(
                            EOGroup(
                                file.name.replace(self.EXTENSION, ""), *map(EOVariable, xr.open_dataset(file).values())
                            ),
                        )
            eogroups[group_name] = EOGroup(group_name, variables=map(EOVariable, variables), groups=subgroups)
        coords = eogroups["coordinates"]
        del eogroups["coordinates"]
        return EOProduct(self._name, coords, *eogroups.values())


class ZarrReader(EOProductStore):
    _store: None

    def __init__(self, store):
        self._store = store

    def _walk(self, group):
        groups = {name: self._walk(gr) for name, gr in group.groups()}
        variables = [EOVariable(var, name=name, attrs=var.attrs) for name, var in group.arrays()]
        coords = groups.get("coordinates")
        if coords:
            del groups["coordinates"]
        return EOGroup(group.basename, *variables, coords=coords, groups=groups.values(), attrs=group.attrs)

    def read(self) -> EOProduct:
        root = zarr.open_group(self._store)
        eogroups = {name: self._walk(group) for name, group in root.items()}
        coords = eogroups["coordinates"]
        del eogroups["coordinates"]
        return EOProduct(pathlib.Path(self._store.path).name, coords, *eogroups.values(), attrs=root.attrs)


class ZarrWriter(EOProductStore):
    _store: None

    def __init__(self, store):
        self._store = store

    def _set_attr(self, zarr_node, attrs):
        for attr, value in attrs:
            zarr_node.attrs[attr] = value

    def _zar_group(self, parent_node, eogroup):
        if eogroup is None:
            return
        current_node = parent_node.create_group(eogroup.name)
        self._set_attr(current_node, eogroup.attrs)

        for eovar in eogroup:
            current_node.array(eovar.name, eovar._ndarray.to_masked_array())

        for eogr in (eogroup.coords, *eogroup.groups.values()):
            self._zar_group(current_node, eogr)

    def write(self, product: EOProduct):
        root = zarr.group(store=self._store)
        self._set_attr(root, product.attrs)

        for eogroup in (product.coords, *product.values()):
            self._zar_group(root, eogroup)
