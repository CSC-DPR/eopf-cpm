from types import TracebackType
from typing import Any, Iterable, MutableMapping, Optional, Type, Union

from eopf.exceptions import InvalidProductError, StoreNotDefinedError, StoreNotOpenError

from ..store.abstract import EOProductStore, StorageStatus
from .eo_container import EOContainer
from .eo_group import EOGroup
from .eo_variable import EOVariable


class EOProduct(EOContainer):
    """A EOProduct contains EOGroups (and throught them their EOVariables), linked to its EOProductStore (if existing).

    Read and write both dynamically or on demand to the EOProductStore.
    It can be used in a dictionary like manner with relative and absolute paths.
    It has personal attributes and both personal and inherited coordinates.

    Parameters
    ----------
    name: str
        name of this product
    store_or_path_url: Union[str, EOProductStore], optional
        a EOProductStore or a string to create to a EOZarrStore
    attrs: dict[str, Any], optional
        global attributes of this product

    See Also
    --------
    eopf.product.conveniences.init_product
    """

    MANDATORY_FIELD = ("measurements", "coordinates")

    def __init__(
        self,
        name: str,
        store_or_path_url: Optional[Union[str, EOProductStore]] = None,
        attrs: Optional[MutableMapping[str, Any]] = None,
    ) -> None:
        EOContainer.__init__(self, attrs=attrs)
        self._name: str = name
        self._store: Optional[EOProductStore] = None
        self.__set_store(store_or_path_url=store_or_path_url)

    @property
    def _is_root(self) -> "bool":
        return True

    @property
    def attributes(self) -> dict[str, Any]:
        """Attributes

        See Also
        --------
        EOContainer.attrs
        """
        return self.attrs

    # docstr-coverage: inherited
    @property
    def product(self) -> "EOProduct":
        return self

    # docstr-coverage: inherited
    @property
    def store(self) -> Optional[EOProductStore]:
        return self._store

    # docstr-coverage: inherited
    @property
    def path(self) -> str:
        return "/"

    # docstr-coverage: inherited
    @property
    def relative_path(self) -> Iterable[str]:
        return []

    # docstr-coverage: inherited
    @property
    def name(self) -> str:
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
        from ..formatting import renderer

        return renderer("product.html", product=self)

    def _add_local_variable(self, name: str, data: Optional[Any] = None, **kwargs: Any) -> EOVariable:
        raise InvalidProductError("Products can't directly store variables.")

    def open(
        self, *, store_or_path_url: Optional[Union[EOProductStore, str]] = None, mode: str = "r", **kwargs: Any
    ) -> "EOProduct":
        """Setup the store to be readable or writable

        if store_or_path_url is given, the store is override by the new one.

        Parameters
        ----------
        store_or_path_url: EOProductStore or str, optional
            the new store or a path url the target file system
        mode: str, optional
            mode to open the store
        **kwargs: Any
            extra kwargs to open the store
        """
        if store_or_path_url is not None:
            self.__set_store(store_or_path_url=store_or_path_url)
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        self.store.open(mode=mode, **kwargs)
        if mode in ["r"]:
            group = self.store[""]
            self.attrs.update(group.attrs)
        return self

    def is_valid(self) -> bool:
        """Check if the product is a valid eopf product

        Returns
        -------
        bool

        See Also
        --------
        EOProduct.validate"""
        return all(key in self for key in self.MANDATORY_FIELD)

    def validate(self) -> None:
        """check if the product is a valid eopf product, raise an error if is not a valid one

        Raises
        ------
        InvalidProductError
            If the product not follow the harmonized common data model

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
        if self.store is None:  # pragma: no cover
            raise StoreNotDefinedError("Store must be defined")
        self.store.close()

    def _create_structure(self, group: Union[EOGroup, tuple[str, EOGroup]], level: int) -> None:
        if isinstance(group, tuple):
            group = group[1]
        for v in group.variables:
            print("|" + " " * level + "└──", v[0])
        for g in group.groups:
            print("|" + " " * level + "├──", g[0])
            self._create_structure(g, level + 2)

    def tree(self, interactive: bool = True) -> None:
        """Display the hierarchy of the product.
        Default to an interactive html representation  in interactive shell (ex : jupiterhub).
        If unavailable will print to console (STDOUT).

        Parameters
        ----------
        interactive: bool, optional
            prefer an interactive html representation to a console print.
        """
        try:  # pragma: no cover
            from IPython import display, get_ipython
            from IPython.terminal.interactiveshell import TerminalInteractiveShell

            py_type = get_ipython()  # Recover python environment from which this is used
            if interactive and py_type and not isinstance(py_type, TerminalInteractiveShell):
                # Return EOProduct if environment is interactive
                display.display(display.HTML(self._repr_html_()))
                return
        except ModuleNotFoundError:  # pragma: no cover
            import warnings

            warnings.warn("IPython not found")
        # Iterate and print EOProduct structure otherwise (CLI)
        for name, group in self._groups.items():
            print(f"├── {name}")
            self._create_structure(group, level=2)
        return

    @property
    def coordinates(self) -> EOGroup:
        """EOGroup: Coordinates mandatory group"""
        coords = self.get("coordinates")
        if not isinstance(coords, EOGroup):  # pragma: no cover (theorically impossible)
            raise InvalidProductError("coordinates must be defined at product level and must be an EOGroup")
        return coords

    # docstr-coverage: inherited
    def write(self) -> None:
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        if self.store.status == StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open")
        self.store.write_attrs("", attrs=self.attrs)
        return super().write()

    # docstr-coverage: inherited
    def load(self) -> None:
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        if self.store.status == StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open")
        return super().load()
