from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Optional

from eopf.exceptions import StoreNotDefinedError
from eopf.product.core.eo_container import EOContainer
from eopf.product.core.eo_object import EOObject

from ..formatting import renderer
from .eo_variable import EOVariable

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_product import EOProduct


class EOGroup(EOContainer, EOObject):
    """A group of EOVariable and sub EOGroup, linked to their EOProduct's EOProductStore (if existing).

    Read and write both dynamically, or on demand to the EOProductStore.
    Can be used in a dictionary like manner with relatives and absolutes paths.
    Has personal attributes and both personal and inherited coordinates.
    Coordinates should be set in the given key-value pair format: {"path/under/coordinates/group" : array_like}.
    About dimension, if a value is a path under the coordinates group, the coordinates is automatically associated.

    Parameters
    ----------
    name: str, optional
        name of this group
    product: EOProduct, optional
        product top level
    relative_path: Iterable[str], optional
        list like of string representing the path from the product
    dataset: xarray.Dataset, optional
        data for :obj:`EOVariable`
    attrs: MutableMapping[str, Any], optional
        attributes to assign
    dims: tuple[str], optional
        dimensions to assign

    Raises
    ------
    TypeError
        Trying to set a dataset with non str keys or with non dataset object

    See Also
    --------
    xarray.Dataset
    """

    def __init__(
        self,
        name: str = "",
        parent: "Optional[EOProduct]" = None,
        variables: Optional[dict[str, EOVariable]] = None,
        attrs: Optional[MutableMapping[str, Any]] = None,
        dims: tuple[str, ...] = tuple(),
    ) -> None:
        EOContainer.__init__(self, attrs=attrs)

        if variables is None:
            variables = dict()
        else:
            if not isinstance(variables, dict):
                raise TypeError("dataset parameters must be a dictionary")
            for key in variables:
                if not isinstance(key, str):
                    raise TypeError(f"The dataset key {str(key)} is type {type(key)} instead of str")
        self._variables = variables
        EOObject.__init__(self, name, parent, dims=dims)

    def _get_item(self, key: str) -> "EOObject":
        if key in self._variables:
            return self._variables[key]

        return super()._get_item(key)

    def __delitem__(self, key: str) -> None:
        if key in self._variables:
            del self._variables[key]
        else:
            super().__delitem__(key)

    def __iter__(self) -> Iterator[str]:
        yield from super().__iter__()
        if self._variables is not None:
            yield from self._variables.keys()

    def __contains__(self, key: object) -> bool:
        return super().__contains__(key) or (key in self._variables)

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOGroup]{hex(id(self))}"

    def _repr_html_(self) -> str:  # pragma: no cover
        return renderer("group.html", group=self)

    def to_product(self) -> "EOProduct":
        """Convert this group to a product

        Returns
        -------
        EOProduct
        """
        raise NotImplementedError

    @property
    def groups(self) -> Iterator[tuple[str, "EOGroup"]]:
        """Iterator over the sub EOGroup of this EOGroup"""
        for key, value in self.items():
            if isinstance(value, EOGroup):
                yield key, value

    @property
    def variables(self) -> Iterator[tuple[str, EOVariable]]:
        """Iterator over the couples variable_name, EOVariable of this EOGroup"""
        for key, value in self.items():
            if isinstance(value, EOVariable):
                yield key, value

    def _add_local_variable(self, name: str, data: Optional[Any] = None, **kwargs: Any) -> EOVariable:

        if not isinstance(data, EOVariable):
            variable = EOVariable(name, data, self, **kwargs)
        else:
            variable = data
        self._variables[name] = variable

        return variable

    def write(self) -> None:
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        super().write()
        for var_name, item in self._variables.items():
            self.store[self._store_key(var_name)] = item

    def _find_by_dim(self, dims: Iterable[str], shape: Optional[tuple[int]] = None) -> list["EOObject"]:
        var_found = super()._find_by_dim(dims, shape)
        for path in self:
            var_found += self[path]._find_by_dim(dims, shape)
        return var_found
