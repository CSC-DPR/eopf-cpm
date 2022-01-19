import enum
import warnings
from abc import abstractmethod
from collections.abc import MutableMapping
from typing import Any, Iterable, Optional

import fsspec
import xarray

from eopf.exceptions.warnings import AlreadyClose, AlreadyOpen


class StorageStatus(enum.Enum):
    OPEN = "open"
    CLOSE = "close"


class EOProductStore(MutableMapping[str, Any]):
    """"""

    sep: str = "/"

    @property
    @abstractmethod
    def map(self) -> fsspec.FSMap:
        """"""

    @property
    def is_readable(self) -> bool:
        return True

    @property
    def is_writeable(self) -> bool:
        return True

    @property
    def is_listable(self) -> bool:
        return True

    @property
    def is_erasable(self) -> bool:
        return True

    @property
    def status(self) -> StorageStatus:
        return self._status

    def __init__(self, url: str) -> None:
        self.url = url
        self._status = StorageStatus.CLOSE

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        """"""
        if self._status == StorageStatus.OPEN:
            warnings.warn(AlreadyOpen())
        self._status = StorageStatus.OPEN

    def close(self) -> None:
        """"""
        if self._status == StorageStatus.CLOSE:
            warnings.warn(AlreadyClose())
        self._status = StorageStatus.CLOSE

    @abstractmethod
    def listdir(self, path: Optional[str] = None) -> Iterable[str]:
        """"""

    @abstractmethod
    def rmdir(self, path: Optional[str] = None) -> None:
        """"""

    @abstractmethod
    def clear(self) -> None:
        """"""

    @abstractmethod
    def getsize(self, path: Optional[str] = None) -> None:
        """"""

    @abstractmethod
    def dir_path(self, path: Optional[str] = None) -> None:
        """"""

    @abstractmethod
    def is_group(self, path: str) -> bool:
        """"""

    @abstractmethod
    def is_variable(self, path: str) -> bool:
        """"""

    @abstractmethod
    def add_group(self, name: str, relative_path: list[str] = []) -> None:
        """"""

    @abstractmethod
    def add_variables(self, name: str, dataset: xarray.Dataset, relative_path: list[str] = []) -> None:
        """"""

    def __hash__(self) -> int:
        return id(self)
