import weakref
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Iterable, Optional

from eopf.exceptions import InvalidProductError
from eopf.product.core.eo_abstract import EOAbstract
from eopf.product.store.abstract import EOProductStore
from eopf.product.utils import join_eo_path

if TYPE_CHECKING:
    from eopf.product.core.eo_group import EOGroup
    from eopf.product.core.eo_product import EOProduct
    from eopf.product.core.eo_variable import EOVariable


class EOObject(EOAbstract):
    def __init__(
        self,
        name: str,
        product: "Optional[EOProduct]" = None,
        relative_path: Optional[Iterable[str]] = None,
    ) -> None:
        self._name: str = name
        self._relative_path: tuple[str, ...] = tuple(relative_path) if relative_path is not None else tuple()
        if product is not None and not isinstance(product, weakref.ProxyType):
            product = weakref.proxy(product)
        self._product: "Optional[EOProduct]" = product

    @property
    @abstractmethod
    def attrs(self) -> dict[str, Any]:
        ...

    @property
    def name(self) -> str:
        """name of this variable"""
        return self._name

    @property
    def path(self) -> str:
        """path from the top level product to this EOObject"""
        return join_eo_path(*self.relative_path, self.name)

    @property
    def product(self) -> "EOProduct":
        if self._product is None:
            raise InvalidProductError("Undefined product")
        return self._product

    @property
    def relative_path(self) -> Iterable[str]:
        """relative path of this EOObject"""
        return self._relative_path

    @property
    def store(self) -> Optional[EOProductStore]:
        """direct accessor to the product store"""
        return self.product.store

    @property
    def coordinates(self) -> "EOGroup":
        """Coordinates defined by this object (does not consider inheritance)."""
        from .eo_group import EOGroup

        coord = self.product.coordinates[self.path]
        if not isinstance(coord, EOGroup):
            raise TypeError(f"EOVariable coordinates type must be EOGroup instead of {type(coord)}.")
        return coord

    def get_coordinate(self, name: str, context: Optional[str] = None) -> "EOVariable":
        """Get coordinate name in the path context (context default to this object).
        Consider coordinate inheritance.
        """
        if context is None:
            context = self.path
        return self.product.get_coordinate(name, context)
