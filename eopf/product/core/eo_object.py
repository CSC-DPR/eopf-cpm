import weakref
from typing import TYPE_CHECKING, Any, Iterable, Optional

from eopf.exceptions import StoreNotDefinedError
from eopf.product.core.eo_abstract import EOAbstract
from eopf.product.core.eo_container import EOContainer
from eopf.product.store.abstract import EOProductStore
from eopf.product.utils import join_path

if TYPE_CHECKING:
    from eopf.product.core.eo_group import EOGroup
    from eopf.product.core.eo_product import EOProduct
    from eopf.product.core.eo_variable import EOVariable


class EOObject(EOAbstract):
    def __init__(
        self,
        name: str,
        product: "EOProduct",
        relative_path: Optional[Iterable[str]] = None,
        attrs: Optional[dict[str, Any]] = None,
    ) -> None:
        self._name: str = name
        self._relative_path: tuple[str, ...] = tuple(relative_path) if relative_path is not None else tuple()
        self._product: EOProduct = weakref.proxy(product) if not isinstance(product, weakref.ProxyType) else product
        self._attrs: dict[str, Any] = attrs or dict()

    @property
    def name(self) -> str:
        """name of this variable"""
        return self._name

    @property
    def _path(self) -> str:
        """path from the top level product to this EOObject"""
        if self._store is None:
            raise StoreNotDefinedError("Store must be defined")
        return join_path(*self._relative_path, self._name, sep=self._store.sep)

    @property
    def _store(self) -> Optional[EOProductStore]:
        """direct accessor to the product store"""
        return self._product._store

    @property
    def coordinates(self) -> "EOGroup":
        """Coordinates defined by this object (does not consider inheritance)."""
        coord = self._product.coordinates[self._path]
        if not isinstance(coord, EOContainer):
            raise TypeError(f"EOVariable coordinates type must be EOGroup instead of {type(coord)}.")
        return coord

    def get_coordinate(self, name: str, context: Optional[str] = None) -> "EOVariable":
        """Get coordinate name in the path context (context default to this object).
        Consider coordinate inheritance.
        """
        if context is None:
            context = self._path
        return self._product.get_coordinate(name, context)

    @property
    def attrs(self) -> dict[str, Any]:
        """Attributes defined by this object (does not consider inheritance)."""
        return self._attrs
