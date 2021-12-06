"""
Define Store Objects for EOProduct.
"""

from abc import ABC, abstractclassmethod, abstractmethod
from typing import Type

from fsspec.spec import AbstractFileSystem

from .core import EOProduct


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
