from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Optional, Union

from eopf.exceptions import StoreNotDefinedError
from eopf.product.core.eo_container import EOContainer
from eopf.product.core.eo_object import EOObject

from ..formatting import renderer
from .eo_variable import EOVariable

if TYPE_CHECKING:  # pragma: no cover
    from eopf.product.core.eo_product import EOProduct


class EOGroup(EOContainer, EOObject):
    """
    A group of EOVariable and sub EOGroup, linked to their EOProduct's EOProductStore (if existing).

    Read and write both dynamically, or on demand to the EOProductStore.
    Can be used in a dictionary like manner with relatives and absolutes paths.
    Has personal attributes and both personal and inherited coordinates.

    """

    def __init__(
        self,
        name: str = "",
        product: "Optional[EOProduct]" = None,
        relative_path: Optional[Iterable[str]] = None,
        variables: Optional[dict[str, EOVariable]] = None,
        attrs: Optional[dict[str, Any]] = None,
        coords: MutableMapping[str, Any] = {},
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
        EOObject.__init__(self, name, product, relative_path, coords=coords, retrieve_dims=dims)

    def _get_item(self, key: str) -> Union[EOVariable, "EOGroup"]:
        """Find and return an EOVariable or EOGroup from the given key.

        If store is defined and key not already loaded in this group,
        the object data is loaded from it.
        Parameters
        ----------
        key: str
            path of the EOVariable or EOGroup
        """
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

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOGroup]{hex(id(self))}"

    def _repr_html_(self) -> str:
        return renderer("group.html", group=self)

    def to_product(self) -> "EOProduct":
        """Convert this group to a product"""
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

        """Construct and add an eovariable to this group

        if store is defined and open, the eovariable it's directly write by the store.
        Parameters
        ----------
        name: str
            name of the future group
        data: any, optional
            data like object use to create dataarray or dataarray
        **kwargs: any
            DataArray extra parameters if data is not a dataarray
        Returns
        -------
        EOVariable
            newly created EOVariable
        """

        if not isinstance(data, EOVariable):
            variable = EOVariable(name, data, self.product, relative_path=[*self._relative_path, self._name], **kwargs)
        else:
            variable = data
        self._variables[name] = variable
        return variable

    def write(self) -> None:
        """
        write non synchronized subgroups, variables to the store

        the store must be opened to work
        See Also
        --------
        EOProduct.open
        """
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        super().write()
        for var_name in self._variables:
            self.store[self._store_key(var_name)] = self._variables[var_name]

    def __contains__(self, key: object) -> bool:
        return super().__contains__(key) or (key in self._variables)
