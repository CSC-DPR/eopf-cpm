from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterator

from eopf.exceptions import StoreNotOpenError
from eopf.product.store import EOProductStore
from eopf.product.store.abstract import StorageStatus

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core import EOVariable


class EOExtractDimAccessor(EOProductStore):
    """
    Accessor representation to Extract Dimension from another type of store.

    Parameters
    ----------
    url: str
        path or url to access

    Attributes
    ----------
    url: str
        path or url to access
    _store_cls: type[EOProductStore]
        EOProductStore class to deal with
    _extract_dim: str
        dimension name to extract
    """

    _store_cls: type[EOProductStore]
    _extract_dim: str

    def __init__(self, url: str, **kwargs: Any) -> None:
        self._store = self._store_cls(url, **kwargs)
        super().__init__(url)

    # docstr-coverage: inherited
    def open(self, mode: str = "r", **kwargs: Any) -> None:
        self._store.open(mode=mode, **kwargs)

    # docstr-coverage: inherited
    def close(self) -> None:
        self._store.close()

    # docstr-coverage: inherited
    def iter(self, path: str) -> Iterator[str]:
        return self._store.iter(path)

    def __iter__(self) -> Iterator[str]:
        return self._store.__iter__()

    def __getitem__(self, key: str) -> "EOVariable":
        from eopf.product.core import EOGroup, EOVariable

        eo_var = self._store.__getitem__(key)
        if not isinstance(eo_var, (EOVariable, EOGroup)):
            raise NotImplementedError()
        obj = eo_var[self._extract_dim]
        if isinstance(obj, EOVariable):
            return obj
        return EOVariable(data=obj)

    def __setitem__(self, key: str, value: Any) -> None:
        if self._status == StorageStatus.CLOSE:
            raise StoreNotOpenError()
        raise NotImplementedError

    def __len__(self) -> int:
        return self._store.__len__()

    # docstr-coverage: inherited
    def write_attrs(self, group_path: str, attrs: MutableMapping[str, Any] = {}) -> None:
        return self._store.write_attrs(group_path, attrs)

    # docstr-coverage: inherited
    def is_group(self, path: str) -> bool:
        return self._store.is_group(path)

    # docstr-coverage: inherited
    def is_variable(self, path: str) -> bool:
        return self._store.is_variable(path)
