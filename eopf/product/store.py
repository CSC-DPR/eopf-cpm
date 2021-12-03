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
    def from_product(cls, eoproduct: EOProduct) -> "EOProductStore":
        """ """

    @abstractmethod
    def to_product(self, klass: Type[EOProduct]) -> EOProduct:
        """ """

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
