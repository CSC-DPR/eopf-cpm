import enum
import warnings
from abc import abstractmethod
from collections.abc import MutableMapping
from typing import Any, Iterable, Iterator, Optional

import xarray

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
    def listdir(self, path: Optional[str] = None) -> Iterable[str]:
        """List the given path on the store, or the root if no path.

        Parameters
        ----------
        path: str, optional
            path to list on the store

        Returns
        -------
        typing.Iterable[str]
            List like object containing the path of object inside

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """

    @abstractmethod
    def rmdir(self, path: Optional[str] = None) -> None:
        """Remove the given path on the store, or the root if no path.

        Parameters
        ----------
        path: str, optional
            path to remove on the store

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """

    @abstractmethod
    def clear(self) -> None:
        """Clear the store from root path

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """

    @abstractmethod
    def getsize(self, path: Optional[str] = None) -> None:
        """Return size under the path or root if no path given

        Parameters
        ----------
        path: str, optional
            path to get size on the store

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """

    @abstractmethod
    def dir_path(self, path: Optional[str] = None) -> None:
        """Return directory path of the given path or root

        Parameters
        ----------
        path: str, optional
            path to get directory on the store

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """

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
    def add_group(self, name: str, relative_path: Iterable[str] = [], attrs: MutableMapping[str, Any] = {}) -> None:
        """Write a group over the store

        Parameters
        ----------
        name: str
            name of the group
        relative_path: typing.Iterable[str], optional
            list of all parents from root level
        attrs: collections.abc.MutableMapping[str, Any], optional
            dict like representation of attributes to assign

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """

    @abstractmethod
    def add_variables(self, name: str, dataset: xarray.Dataset, relative_path: Iterable[str] = []) -> None:
        """Write variables over the store from :obj:`xarray.Dataset`

        Parameters
        ----------
        name: str
            name of the dataset
        relative_path: typing.Iterable[str], optional
            list of all parents from root level

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """

    @abstractmethod
    def update_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        """Update attrs in the store

        Parameters
        ----------
        group_path: str
            path of the object to write attributes
        attrs: collections.abc.MutableMapping[str, Any], optional
            dict like representation of attributes to write

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """

    @abstractmethod
    def delete_attr(self, group_path: str, attr_name: str) -> None:
        """Delete the specific attributes

        Parameters
        ----------
        group_path: str
            path of the object corresponding to the attributes
        attr_name: str
            name of the attributes to delete

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
        typing.Iterator[str]
            An iterator of the paths inside the given path

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        """

    @abstractmethod
    def get_data(self, path: str) -> tuple[Optional[xarray.Dataset], dict[str, Any]]:
        """Retrieve the datas of a group indexed by the given path.

        Parameters
        ----------
        path: str
            path of the group that's containe the data

        Returns
        -------
        xarray.Dataset or None
            data of the group
        dict[str, Any]
            attributes of the group

        Raises
        ------
        StoreNotOpenError
            If the store is closed
        KeyError
            There is no data at the given path
        TypeError
            You try to extract data from variable directly
        """

    def __hash__(self) -> int:
        return id(self)
