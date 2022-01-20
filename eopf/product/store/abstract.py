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
    """abstract store representation to access to a files on the given URL

    Attributes
    ----------
    status
    is_readable
    is_writeable
    is_listable
    is_erasable
    url: str
        url to the target store
    sep: str
        file separator
    """

    sep: str = "/"

    @property
    @abstractmethod
    def map(self) -> fsspec.FSMap:
        """FSMap accessor"""

    @property
    def is_readable(self) -> bool:
        """this store can be read or not"""
        return True

    @property
    def is_writeable(self) -> bool:
        """this store can be write or not"""
        return True

    @property
    def is_listable(self) -> bool:
        """this store can be list or not"""
        return True

    @property
    def is_erasable(self) -> bool:
        """this store can be erase or not"""
        return True

    @property
    def status(self) -> StorageStatus:
        """give the current status (open or close) of this store"""
        return self._status

    def __init__(self, url: str) -> None:
        self.url = url
        self._status = StorageStatus.CLOSE

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        """open the store in the given mode

        Parameters
        ----------
        mode: str, optional
            mode to open the store
        **kwargs: Any
            extra kwargs of open on librairy used
        """
        if self._status == StorageStatus.OPEN:
            warnings.warn(AlreadyOpen())
        self._status = StorageStatus.OPEN

    def close(self) -> None:
        """close the store"""
        if self._status == StorageStatus.CLOSE:
            warnings.warn(AlreadyClose())
        self._status = StorageStatus.CLOSE

    @abstractmethod
    def listdir(self, path: Optional[str] = None) -> Iterable[str]:
        """list the given path on the store, or the root if no path.

        Parameters
        ----------
        path: str, optional
            path to list on the store
        """

    @abstractmethod
    def rmdir(self, path: Optional[str] = None) -> None:
        """remove the given path on the store, or the root if no path.

        Parameters
        ----------
        path: str, optional
            path to remove on the store
        """

    @abstractmethod
    def clear(self) -> None:
        """clear all the store from root path"""

    @abstractmethod
    def getsize(self, path: Optional[str] = None) -> None:
        """return size under the path or root if no path given

        Parameters
        ----------
        path: str, optional
            path to get size on the store
        """

    @abstractmethod
    def dir_path(self, path: Optional[str] = None) -> None:
        """return directory path of the given path or root

        Parameters
        ----------
        path: str, optional
            path to get directory on the store
        """

    @abstractmethod
    def is_group(self, path: str) -> bool:
        """check if the given path under root corresponding to a group representation

        Parameters
        ----------
        path: str
            path to check
        """

    @abstractmethod
    def is_variable(self, path: str) -> bool:
        """check if the given path under root corresponding to a variable representation

        Parameters
        ----------
        path: str
            path to check
        """

    @abstractmethod
    def add_group(self, name: str, relative_path: list[str] = []) -> None:
        """write a group over the store

        Parameters
        ----------
        name: str
            name of the group
        relative_path: list[str], optional
            list of all parents from root
        """

    @abstractmethod
    def add_variables(self, name: str, dataset: xarray.Dataset, relative_path: list[str] = []) -> None:
        """write variables over the store

        Parameters
        ----------
        name: str
            name of the dataset
        relative_path: list[str], optional
            list of all parents from root
        """

    def __hash__(self) -> int:
        return id(self)
