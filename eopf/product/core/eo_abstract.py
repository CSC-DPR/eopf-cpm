from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Iterable, Optional

from eopf.product.store import EOProductStore

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_product import EOProduct


class EOAbstract(ABC):  # pragma: no cover
    """Interface implemented by all EO class (EOProduct, EOGroup, EOVariable) and
    their parents (EOContainer, EObject).
    Declare product affiliation and path access.

    Notes
    -----
        EOVariableOperatorsMixin doesn't inherit this class, being only a mixin class for EOVariable.
    """

    @property
    @abstractmethod
    def product(self) -> "EOProduct":
        """EOProduct: Product related to this object.

        Raises
        ------
        InvalidProductError
            If this object doesn't have a (valid) product.
        """

    @property
    @abstractmethod
    def store(self) -> Optional[EOProductStore]:
        """EOProductStore or None: The store associated to its product
        or None if there is no store

        Raises
        ------
        InvalidProductError
            If this object doesn't have a (valid) product.
        """

    @property
    @abstractmethod
    def path(self) -> str:
        """str: Path from the top level EOProduct to this object.
        It's a string following Linux path conventions."""

    @property
    @abstractmethod
    def relative_path(self) -> Iterable[str]:
        """Iterable[str]: Relative path of this object.
        It's the set of the names of this object parents (Product name as '/')."""

    @property
    @abstractmethod
    def name(self) -> str:
        """str: Name of this object. Empty string if unnamed."""

    @property
    @abstractmethod
    def attrs(self) -> dict[str, Any]:
        """dict[str, Any]: Dictionary of this EOObject attributes."""
