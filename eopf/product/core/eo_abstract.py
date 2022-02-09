from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Iterable, Optional

from eopf.product.store import EOProductStore

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_product import EOProduct
    from eopf.product.core.eo_variable import EOVariable


class EOAbstract(ABC):  # pragma: no cover
    """
    Interface implemented by all EO class (EOProduct, EOGroup, EOVariable) and
    their parents (EOContainer, EObject).
    Declare product affiliation and path access.

    Note : EOVariableOperatorsMixin doesn't inherit this class, being only a mixin class for EOVariable.
    """

    @property
    @abstractmethod  # Order of these matter, you must make abstract then read-only.
    def product(self) -> "EOProduct":
        """Get this object product.
        Raises
        ------
        InvalidProductError
            If this object doesn't have a (valid) product.
        """
        ...

    @property
    @abstractmethod
    def store(self) -> Optional[EOProductStore]:
        """Get the store of this object from it's product.
        Return None if the product doesn't have a store.

        Raises
        ------
        InvalidProductError
            If this object doesn't have a (valid) product.
        """
        ...

    @property
    @abstractmethod
    def path(self) -> str:
        """Path from the top level EOProduct to this object.
        It's a string following Linux path conventions."""
        ...

    @property
    @abstractmethod
    def relative_path(self) -> Iterable[str]:
        """Relative path of this object.
        It's the set of the names of this object parents (Product name as '/')."""
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of this object. Empty string if unnamed."""
        ...

    @abstractmethod
    def get_coordinate(self, name: str, context: Optional[str] = None) -> "EOVariable":
        """Get coordinate in the context. Consider coordinate inheritance.
        Allow to modify the coordinate.

        Parameters
        ----------
        name: str
            name of th ecoordinate
        context: str
            path of the context (default to this object)

        Returns
        -------
        EOVariable
            variable containing this coordinate.
        Raises
        ------
        InvalidProductError
            If this object doesn't have a (valid) product.
        KeyError
            If there is no coordinate name in the context
        """
        ...

    @property
    @abstractmethod
    def attrs(self) -> dict[str, Any]:
        """
        Dictionary of this EOObject attributes.
        """
        ...
