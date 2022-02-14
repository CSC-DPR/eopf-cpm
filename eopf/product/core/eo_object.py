import itertools as it
import weakref
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Iterable, MutableMapping, Optional

from eopf.exceptions import EOObjectMultipleParentError, InvalidProductError
from eopf.product.core.eo_abstract import EOAbstract
from eopf.product.store.abstract import EOProductStore
from eopf.product.utils import join_eo_path, join_path

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_product import EOProduct
    from eopf.product.core.eo_variable import EOVariable


_DIMENSIONS_PATHS = "_EOPF_DIMENSIONS_PATHS"
_DIMENSIONS_NAME = "_EOPF_DIMENSIONS"


class EOObject(EOAbstract):
    """Abstract class implemented by EOGroup and EOVariable.
    Provide local attributes, and both local and inherited coordinates.
    Implement product affiliation and path access.

    Parameters
    ----------
    name: str, optional
        name of this group
    product: EOProduct, optional
        product top level
    relative_path: Iterable[str], optional
        list like of string representing the path from the product
    coords: MutableMapping[str, Any], optional
        coordinates to assign
    retrive_dims: tuple[str], optional
        dimensions to assign
    """

    def __init__(
        self,
        name: str,
        product: Optional["EOProduct"] = None,
        relative_path: Optional[Iterable[str]] = None,
        coords: MutableMapping[str, Any] = {},
        retrieve_dims: tuple[str, ...] = tuple(),
    ) -> None:

        self._name: str = ""
        self._relative_path: tuple[str, ...] = tuple()
        self._product: Optional["EOProduct"] = None
        self._repath(name, product, relative_path)
        self.assign_coords(coords=coords)
        self.assign_dims(retrieve_dims=retrieve_dims)

    def assign_dims(self, retrieve_dims: Iterable[str]) -> None:
        """Assign dimension to this object

        Parameters
        ----------
        retrive_dims: Iterable[str], optional
            dimensions to assign
        """
        for key in retrieve_dims:
            path, _, dim = key.rpartition("/")
            self.attrs.setdefault(_DIMENSIONS_PATHS, []).append(path)
            self.attrs.setdefault(_DIMENSIONS_NAME, []).append(dim)

    def assign_coords(self, coords: MutableMapping[str, Any] = {}, **kwargs: Any) -> None:
        """Assign coordinates to this object

        Coordinates should be set in the given key-value pair format: {"path/under/coordinates/group" : array_like}.

        Parameters
        ----------
        coords: MutableMapping[str, Any] optional
            coordinates to assign
        """
        for path, coords_value in it.chain(coords.items(), kwargs.items()):
            self.product.coordinates.add_variable(path, data=coords_value)
            self.assign_dims([path])

    def _repath(self, name: str, product: "Optional[EOProduct]", relative_path: Optional[Iterable[str]]) -> None:
        """Set the name, product and relative_path attributes of this EObject.
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
    def dims(self) -> tuple[str, ...]:
        """tupple[str, ...]: dimensions"""
        return self.attrs.get(_DIMENSIONS_NAME, tuple())

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
    def coordinates(self) -> MappingProxyType[str, Any]:
        """MappingProxyType[str, Any]: Coordinates defined by this object"""
        dims = self.dims
        coords_group = self.product.coordinates
        return MappingProxyType(
            {
                join_path(coord_path, dim): coords_group[join_eo_path(coord_path, dim)]
                for coord_path, dim in zip(self.attrs.get(_DIMENSIONS_PATHS, ["/"] * len(dims)), dims)
            },
        )

    def get_coordinate(self, name: str, context: Optional[str] = None) -> "EOVariable":
        if context is None:
            context = self.path
        return self.product.get_coordinate(name, context)
