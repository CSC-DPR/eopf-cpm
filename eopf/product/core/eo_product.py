from collections.abc import MutableMapping
from types import TracebackType
from typing import Any, Iterable, Optional, Type, Union

from eopf.exceptions import InvalidProductError, StoreNotDefinedError
from eopf.product.utils import join_path, split_path

from ..formatting import renderer
from ..store.abstract import EOProductStore
from .eo_container import EOContainer
from .eo_group import EOGroup
from .eo_variable import EOVariable


class EOProduct(EOContainer, MutableMapping[str, Union["EOVariable", "EOGroup"]]):
    """"""

    MANDATORY_FIELD = ("measurements", "coordinates", "attributes")

    def __init__(self, name: str, store_or_path_url: Optional[Union[str, EOProductStore]] = None) -> None:
        EOContainer.__init__(self)
        self._name: str = name
        self._store: Optional[EOProductStore] = None
        self.__set_store(store_or_path_url=store_or_path_url)

    @property
    def product(self) -> "EOProduct":
        return self

    @property
    def store(self) -> EOProductStore:
        return self._store

    @property
    def path(self) -> str:
        return "/"

    @property
    def relative_path(self) -> Iterable[str]:
        return []

    @property
    def name(self) -> str:
        """name of the product"""
        return self._name

    def __set_store(self, store_or_path_url: Optional[Union[str, EOProductStore]] = None) -> None:
        from ..store.zarr import EOZarrStore

        if isinstance(store_or_path_url, str):
            self._store = EOZarrStore(store_or_path_url)
        elif isinstance(store_or_path_url, EOProductStore):
            self._store = store_or_path_url
        elif store_or_path_url is not None:
            raise TypeError(f"{type(store_or_path_url)} can't be used to instantiate EOProductStore.")

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"[EOProduct]{hex(id(self))}"

    def _repr_html_(self) -> str:
        return renderer("product.html", product=self)

    def open(
        self, *, store_or_path_url: Optional[Union[EOProductStore, str]] = None, mode: str = "r", **kwargs: Any
    ) -> "EOProduct":
        """setup the store to be readable or writable

        if store_or_path_url is given, the store is overwrite by the new one.

        Parameters
        ----------
        store_or_path_url: EOProductStore or str, optional
            the new store or a path url the target file system
        mode: str, optional
            mode to open the store
        **kwargs: Any
            extra kwargs to open the store
        """
        if store_or_path_url:
            self.__set_store(store_or_path_url=store_or_path_url)
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        self.store.open(mode=mode, **kwargs)
        return self

    def load(self) -> None:
        """load all the product in memory"""
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        for key in self.store:
            if key not in self._groups:
                ...

    def is_valid(self) -> bool:
        """check if the product is a valid eopf product
        See Also
        --------
        EOProduct.validate"""
        return all(key in self for key in self.MANDATORY_FIELD)

    def validate(self) -> None:
        """check if the product is a valid eopf product, raise an error if is not a valid one

        See Also
        --------
        EOProduct.is_valid
        """
        if not self.is_valid():
            raise InvalidProductError(f"Invalid product {self}, missing mandatory groups.")

    def __enter__(self) -> "EOProduct":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.store.close()

    def get_coordinate(self, name: str, context: str) -> EOVariable:
        """Get coordinate name in the path context (context default to this object).
        Consider coordinate inheritance.
        """
        sep = self.store.sep
        context_split = split_path(context, sep=sep)
        if context_split[0] != "coordinates":
            context_split = ["coordinates"] + context_split
        while len(context_split) > 0:
            try:
                coord = self[join_path(*context_split, context, sep=sep)]
                if isinstance(coord, EOVariable):
                    # It is valid to have a subgroup with the name of a coordinate of an ancestor.
                    # ex : /coordinate/coord_name et /coordinate/group1/coord_name/obj
                    return coord
            except KeyError:
                pass
        raise KeyError(f"Unknown coordinate {name} in context {context} .")

    def _create_structure(self, group: Union[EOGroup, tuple[str, EOGroup]], level: int) -> None:
        if isinstance(group, tuple):
            group = group[1]
        for v in group.variables:
            print("|" + " " * level + "└──", v[0])
        for g in group.groups:
            print("|" + " " * level + "├──", g[0])
            self._create_structure(g, level + 2)

    def tree(self) -> Union["EOProduct", None]:
        """Display the hierarchy of the product.

        Returns
        ------
        Instance of EOProduct if the environment is interactive (e.g. Jupyter Notebook)
        Oterwise, returns None.
        """
        try:
            from IPython import get_ipython

            if get_ipython():
                return self
        except ModuleNotFoundError:
            import warnings

            warnings.warn("IPython not found")
        for name, group in self._groups.items():
            print(f"├── {name}")
            self._create_structure(group, level=2)
        return None
