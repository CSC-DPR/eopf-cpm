import warnings
from types import MappingProxyType
from typing import TYPE_CHECKING, Iterable, Optional

from xarray.backends import zarr

from eopf.exceptions import (
    EOObjectMultipleParentError,
    InvalidProductError,
    StoreNotDefinedError,
    StoreNotOpenError,
)
from eopf.product.core.eo_abstract import EOAbstract
from eopf.product.store.abstract import EOProductStore, StorageStatus
from eopf.product.utils import join_eo_path, join_path

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_container import EOContainer
    from eopf.product.core.eo_product import EOProduct


# Must match xarray zarr one for cross compatibility.
_DIMENSIONS_NAME = zarr.DIMENSION_KEY


class EOObject(EOAbstract):
    """Abstract class implemented by EOGroup and EOVariable.
    Provide local attributes, and both local and inherited coordinates.
    Implement product affiliation and path access.

    Parameters
    ----------
    name: str, optional
        name of this group
    parent: EOProduct or EOGroup, optional
        parent
    dims: tuple[str], optional
        dimensions to assign
    """

    def __init__(
        self,
        name: str,
        parent: "Optional[EOContainer]" = None,
        dims: tuple[str, ...] = tuple(),
    ) -> None:
        self._name: str = ""
        self._parent: Optional["EOContainer"] = None
        self._repath(name, parent)
        self.assign_dims(dims=dims)

    def assign_dims(self, dims: Iterable[str]) -> None:
        """Assign dimension to this object

        Parameters
        ----------
        dims: Iterable[str], optional
            dimensions to assign
        """
        if dims:
            self.attrs[_DIMENSIONS_NAME] = dims
        elif not dims and _DIMENSIONS_NAME in self.attrs:
            del self.attrs[_DIMENSIONS_NAME]

    def _repath(self, name: str, parent: "Optional[EOContainer]") -> None:
        """Set the name, product and relative_path attributes of this EObject.
        This method won't repath the object in a way that result in it being the child of multiple product,
        or at multiple path.

        Parameters
        ----------
        name: str
            name of this object
        parent: EOProduct or EOGroup, optional
            parent to link to this group

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
    def coordinates(self) -> MappingProxyType[str, "EOObject"]:
        """MappingProxyType[str, "EOObject"]: Coordinates defined by this object"""
        if self.parent is None:
            warnings.warn("Coordinates can't be retrieve from an EOObject outside an EOProduct")
            return MappingProxyType({})
        coords_group = self.product.coordinates
        coords_list = coords_group._find_by_dim(self.dims)
        return MappingProxyType({coord.path: coord for coord in coords_list})

    @property
    def dims(self) -> tuple[str, ...]:
        """tuple[str, ...]: dimensions"""
        return tuple(self.attrs.get(_DIMENSIONS_NAME, tuple()))

    # docstr-coverage: inherited
    @property
    def name(self) -> str:
        return self._name

    @property
    def parent(self) -> "Optional[EOContainer]":
        """
        Parent Container/Product of this object in it's Product.

        Returns
        -------

        """
        return self._parent

    # docstr-coverage: inherited
    @property
    def path(self) -> str:
        if self.parent is None:
            return self.name
        else:
            return join_eo_path(self.parent.path, self.name)

    # docstr-coverage: inherited
    @property
    def product(self) -> "EOProduct":
        if self.parent is None:
            raise InvalidProductError("Undefined product")
        return self.parent.product

    # docstr-coverage: inherited
    @property
    def relative_path(self) -> Iterable[str]:
        rel_path: list[str] = list()
        if self.parent is not None:
            if self.parent._is_root:
                return ["/"]
            rel_path.extend(self.parent.relative_path)
            rel_path.append(self.parent.name)
        return rel_path

    # docstr-coverage: inherited
    @property
    def store(self) -> Optional[EOProductStore]:
        return self.product.store if self.parent is not None else None

    # docstr-coverage: inherited
    def write(self) -> None:
        if self.store is None:  # pragma: no cover
            raise StoreNotDefinedError("Store must be defined")
        if self.store.status == StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open")
        super().write()
        self.store[join_path(*self.relative_path, self.name, sep=self.store.sep)] = self

    def _find_by_dim(self, dims: Iterable[str], shape: Optional[tuple[int, ...]] = None) -> list["EOObject"]:
        for dim_index, dim_name in enumerate(dims):
            if dim_name in self.dims:
                if shape:
                    self_shape = getattr(self, "shape", None)
                    self_dim_index = self.dims.index(dim_name)
                    if not self_shape or shape[dim_index] != self_shape[self_dim_index]:
                        continue
                return [self]
        return []
