import enum
import warnings
from abc import abstractmethod
from collections.abc import MutableMapping
from typing import Any, Iterator

from eopf.exceptions.warnings import AlreadyClose, AlreadyOpen


class StorageStatus(enum.Enum):

    OPEN = "open"
    CLOSE = "close"


class EOProductStore(MutableMapping[str, Any]):
    """Abstract store representation to access to a files on the given URL

    Inherite from MutableMapping to indexes objects with there correponding
    path.

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
    """

    sep: str = "/"

    def __init__(self, url: str) -> None:
        self.url = url
        self._status = StorageStatus.CLOSE

    @property
    def is_readable(self) -> bool:
        """bool: this store can be read or not"""
        return True

    @property
    def is_writeable(self) -> bool:
        """bool: this store can be write or not"""
        return True

    @property
    def is_listable(self) -> bool:
        """bool: this store can be list or not"""
        return True

    @property
    def is_erasable(self) -> bool:
        """bool: this store can be erase or not"""
        return True

    @property
    def status(self) -> StorageStatus:
        """StorageStatus: give the current status (open or close) of this store"""
        return self._status

    def open(self, mode: str = "r", **kwargs: Any) -> None:
        """Open the store in the given mode

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
        """Close the store"""
        if self._status == StorageStatus.CLOSE:
            warnings.warn(AlreadyClose())
        self._status = StorageStatus.CLOSE

    @abstractmethod
    def is_group(self, path: str) -> bool:
        """Check if the given path under root corresponding to a group representation

        Parameters
        ----------
        path: str
            path to check

        Returns
        -------
        bool
            it is a group representation or not
        """

    @abstractmethod
    def is_variable(self, path: str) -> bool:
        """Check if the given path under root corresponding to a variable representation

        Parameters
        ----------
        path: str
            path to check

        Returns
        -------
        bool
            it is a variable representation or not

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """

    @abstractmethod
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        """Update attrs in the store

        Parameters
        ----------
        group_path: str
            path of the object to write attributes
        attrs: MutableMapping[str, Any], optional
            dict like representation of attributes to write

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """

    @abstractmethod
    def iter(self, path: str) -> Iterator[str]:
        """Iter over the given path

        Parameters
        ----------
        path: str
            path to the object to iterate over

        Returns
        -------
        Iterator[str]
            An iterator of the paths inside the given path

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """

    def __delitem__(self, key: str) -> None:
        raise NotImplementedError()

    @staticmethod
    def guess_can_read(file_path: str) -> bool:
        return False
