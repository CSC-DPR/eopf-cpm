import weakref
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Iterable, Optional

from eopf.exceptions import EOObjectMultipleParentError, InvalidProductError
from eopf.product.core.eo_abstract import EOAbstract
from eopf.product.store.abstract import EOProductStore
from eopf.product.utils import join_eo_path

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_group import EOGroup
    from eopf.product.core.eo_product import EOProduct
    from eopf.product.core.eo_variable import EOVariable


class EOObject(EOAbstract):
    """
    Abstract class implemented by EOGroup and EOVariable.
    Provide local attributes, and both local and inherited coordinates.
    Implement product affiliation and path access.
    """

    def __init__(
        self,
        name: str,
        product: "Optional[EOProduct]" = None,
        relative_path: Optional[Iterable[str]] = None,
    ) -> None:
        self._name: str = ""
        self._relative_path: tuple[str, ...] = tuple()
        self._product: "Optional[EOProduct]" = None
        self._repath(name, product, relative_path)

    def _repath(self, name: str, product: "Optional[EOProduct]", relative_path: Optional[Iterable[str]]) -> None:
        """
         Set the name, product and relative_path attributes of this EObject.
         This method won't repath the object in a way that result in it being the child of multiple product,
         or at multiple path.

         Parameters
         ----------
         name: str
         product: Optional[EOProduct]
         relative_path: Optional[Iterable[str]]

        Raises
         ------
         EOObjectMultipleParentError
             If the object has a product and a not undefined attribute is modified.

        """
        if product is not None and not isinstance(product, weakref.ProxyType):
            product = weakref.proxy(product)
        relative_path = tuple(relative_path) if relative_path is not None else tuple()
        # weakref.proxy 'is' only work with another proxy
        if self._product is not None:
            if self._name != "" and self._name != name:
                raise EOObjectMultipleParentError("The EOObject name does not match it's new path")
            if self._relative_path != tuple() and self._relative_path != relative_path:
                raise EOObjectMultipleParentError("The EOObject path does not match it's new parent")
            if self._product is not product:
                raise EOObjectMultipleParentError("The EOObject product does not match it's new parent")

        self._name = name
        self._relative_path = relative_path
        self._product = product

    @property
    @abstractmethod
    def attrs(self) -> dict[str, Any]:  # pragma: no cover
        """
        Dictionary of this EOObject attributes.
        """
        ...

    @property
    def name(self) -> str:
        return self._name

    @property
    def path(self) -> str:
        return join_eo_path(*self.relative_path, self.name)

    @property
    def product(self) -> "EOProduct":
        if self._product is None:
            raise InvalidProductError("Undefined product")
        return self._product

    @property
    def relative_path(self) -> Iterable[str]:
        return self._relative_path

    @property
    def store(self) -> Optional[EOProductStore]:
        return self.product.store

    @property
    def coordinates(self) -> "EOGroup":
        """Coordinates group local to this object (does not consider inheritance).
        Allow adding, removing and modifying coordinates.

        Raises
        ------
        InvalidProductError
            If this object doesn't have a (valid) product.
        KeyError
            If there is no coordinate name in the context
        """
        from .eo_group import EOGroup

        coord = self.product.coordinates[self.path]
        if not isinstance(coord, EOGroup):
            raise TypeError(f"EOVariable coordinates type must be EOGroup instead of {type(coord)}.")
        return coord

    def get_coordinate(self, name: str, context: Optional[str] = None) -> "EOVariable":
        if context is None:
            context = self.path
        return self.product.get_coordinate(name, context)
