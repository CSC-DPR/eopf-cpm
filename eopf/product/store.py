"""
Define Store Objects for EOProduct.
"""

import os
import pathlib
from abc import ABC, abstractclassmethod, abstractmethod
from itertools import groupby
from typing import Type

import xarray
from fsspec.spec import AbstractFileSystem

from .core import EOGroup, EOProduct, EOVariable


class EOProductStore(ABC):
    _fs: AbstractFileSystem
    _path: str

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


def convert_nc_to_product(path, attr_file=None):
    def group_order(file_path: str):
        return file_path.name.split("_")[-1].replace(".nc", "")

    attrs = None
    product_name = pathlib.Path(path)
    product_name = product_name.name.replace(product_name.suffix, "")

    files = list(find_files_paths(path, ".nc"))
    grouped_files = groupby(sorted(files, key=group_order), group_order)
    groups = []
    coords = None
    for group_name, files in grouped_files:
        dts = (
            (xarray.open_dataset(file, decode_times=False, mask_and_scale=False), file.name.replace(file.suffix, ""))
            for file in files
        )
        current_group = EOGroup(group_name)
        for dt, name in dts:
            variables = [EOVariable(value) for value in dt.values()]
            if len(variables) == 1:
                current_group[name] = variables[0]
            else:
                current_group.groups[name] = EOGroup(name, *variables, attrs=dt.attrs, dims=dt.dims)
        if group_name == "coordinates":
            coords = current_group
        else:
            groups.append(current_group)
    attrs = xarray.open_dataset(attr_file, decode_times=False, mask_and_scale=False).attrs if attr_file else None
    product = EOProduct(product_name, coords, *groups, attrs=attrs)
    return product
