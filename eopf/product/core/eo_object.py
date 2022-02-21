import itertools as it
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Iterable, MutableMapping, Optional

from eopf.exceptions import EOObjectMultipleParentError, InvalidProductError
from eopf.product.core.eo_abstract import EOAbstract
from eopf.product.store.abstract import EOProductStore
from eopf.product.utils import join_eo_path, join_path

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_container import EOContainer
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
        parent: "Optional[EOContainer]" = None,
        coords: MutableMapping[str, Any] = {},
        retrieve_dims: tuple[str, ...] = tuple(),
    ) -> None:
        self._name: str = ""
        self._parent: Optional["EOContainer"] = None
        self._repath(name, parent)
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

    def _repath(self, name: str, parent: "Optional[EOContainer]") -> None:
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
        if self._parent is not None:
            if self._name != "" and self._name != name:
                raise EOObjectMultipleParentError("The EOObject name does not match it's new path")
            if self._parent is not parent:
                raise EOObjectMultipleParentError("The EOObject product does not match it's new parent")

        self._name = name
        self._parent = parent

    @property
    def dims(self) -> tuple[str, ...]:
        """tupple[str, ...]: dimensions"""
        return self.attrs.get(_DIMENSIONS_NAME, tuple())

    @property
    def name(self) -> str:
        return self._name

    @property
    def parent(self) -> "Optional[EOContainer]":
        """
        Parent COntainer/Product of this object in it's Product.
        Returns
        -------

        """
        return self._parent

    @property
    def path(self) -> str:
        if self.parent is None:
            return self.name
        else:
            return join_eo_path(self.parent.path, self.name)

    @property
    def product(self) -> "EOProduct":
        if self.parent is None:
            raise InvalidProductError("Undefined product")
        return self.parent.product

    @property
    def relative_path(self) -> Iterable[str]:
        rel_path: list[str] = list()
        if self.parent is not None:
            if self.parent._is_root:
                return ["/"]
            rel_path.extend(self.parent.relative_path)
            rel_path.append(self.parent.name)
        return rel_path

    @property
    def store(self) -> Optional[EOProductStore]:
        return self.product.store

    @property
    def coordinates(self) -> MappingProxyType[str, Any]:
        """MappingProxyType[str, Any]: Coordinates defined by this object"""
        dims = self.dims
        coords_group = self.product.coordinates
        retrieved_coords = {}
        for coord_path, dim in zip(self.attrs.get(_DIMENSIONS_PATHS, ["/"] * len(dims)), dims):
            if (coords := coords_group.get(join_eo_path(coord_path, dim))) is not None:
                retrieved_coords[join_path(coord_path, dim)] = coords
        return MappingProxyType(retrieved_coords)

    def get_coordinate(self, name: str, context: Optional[str] = None) -> "EOVariable":
        if context is None:
            context = self.path
        return self.product.get_coordinate(name, context)
