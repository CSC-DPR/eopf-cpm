from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterable, Optional

from eopf.product.store import EOProductStore

if TYPE_CHECKING:
    from eopf.product.core.eo_product import EOProduct


class EOAbstract(ABC):
    @property
    @abstractmethod  # Order of these matter, you must make abstract then read-only.
    def product(self) -> "EOProduct":
        ...

    @property
    @abstractmethod
    def store(self) -> Optional[EOProductStore]:
        ...

    @property
    @abstractmethod
    def path(self) -> str:
        ...

    @property
    @abstractmethod
    def relative_path(self) -> Iterable[str]:
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        ...
