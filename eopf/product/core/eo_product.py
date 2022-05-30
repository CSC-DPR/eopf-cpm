from types import TracebackType
from typing import TYPE_CHECKING, Any, Iterable, MutableMapping, Optional, Type, Union

from eopf.exceptions import InvalidProductError, StoreNotDefinedError, StoreNotOpenError

from ..store.abstract import EOProductStore, StorageStatus
from ..store.mapping_factory import EOMappingFactory
from ..store.store_factory import EOStoreFactory
from .eo_container import EOContainer
from .eo_group import EOGroup
from .eo_variable import EOVariable

if TYPE_CHECKING:  # pragma: no cover
    from .eo_object import EOObject


class EOProduct(EOContainer):
    """A EOProduct contains EOGroups (and throught them their EOVariables), linked to its EOProductStore (if existing).

    Read and write both dynamically or on demand to the EOProductStore.
    It can be used in a dictionary like manner with relative and absolute paths.
    It has personal attributes and both personal and inherited coordinates.

    Parameters
    ----------
    name: str
        name of this product
    storage: Union[str, EOProductStore], optional
        a EOProductStore or a string to create to a EOZarrStore
    attrs: dict[str, Any], optional
        global attributes of this product

    See Also
    --------
    eopf.product.conveniences.init_product
    """

    MANDATORY_FIELD = ("measurements", "coordinates")
    _TYPE_ATTR_STR = "type"

    def __init__(
        self,
        name: str,
        storage: Optional[Union[str, EOProductStore]] = None,
        attrs: Optional[MutableMapping[str, Any]] = None,
        type: str = "",
        store_factory: Optional[EOStoreFactory] = None,
        mapping_factory: Optional[EOMappingFactory] = None,
    ) -> None:
        if store_factory is None:
            store_factory = EOStoreFactory(default_stores=True)
        if mapping_factory is None:
            mapping_factory = EOMappingFactory(default_mappings=True)
        self._store_factory = store_factory
        self._mapping_factory = mapping_factory
        EOContainer.__init__(self, attrs=attrs)
        self._name: str = name
        self._store: Optional[EOProductStore] = None
        self.__set_store(storage=storage)
        self.__type = ""
        self.__short_names: dict[str, str] = dict()
        self.set_type(type)

    def __delitem__(self, key: str) -> None:
        # Support short name to path conversion
        key = self.short_names.get(key, key)
        if key[0] == "/":
            self.__delitem__(key[1:])
        else:
            super().__delitem__(key)

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

    def __getitem__(self, key: str) -> "EOObject":
        # Support short name to path conversion
        key = self.short_names.get(key, key)
        return super().__getitem__(key)

    def __repr__(self) -> str:
        return f"[EOProduct]{hex(id(self))}"

    def __setitem__(self, key: str, value: "EOObject") -> None:
        # Support short name to path conversion
        key = self.short_names.get(key, key)
        super().__setitem__(key, value)

    def __str__(self) -> str:
        return self.__repr__()

    def add_group(self, name: str, attrs: dict[str, Any] = {}, dims: tuple[str, ...] = tuple()) -> "EOGroup":
        key = self.short_names.get(name, name)
        return super().add_group(key, attrs, dims)

    def add_variable(self, name: str, data: Optional[Any] = None, **kwargs: Any) -> "EOVariable":
        key = self.short_names.get(name, name)
        return super().add_variable(key, data, **kwargs)

    @property
    def attributes(self) -> dict[str, Any]:
        """Attributes

        See Also
        --------
        EOContainer.attrs
        """
        return self.attrs

    @property
    def coordinates(self) -> EOGroup:
        """EOGroup: Coordinates mandatory group"""
        coords = self.get("coordinates")
        if not isinstance(coords, EOGroup):  # pragma: no cover (theorically impossible)
            raise InvalidProductError("coordinates must be defined at product level and must be an EOGroup")
        return coords

    def is_valid(self) -> bool:
        """Check if the product is a valid eopf product

        Returns
        -------
        bool

        See Also
        --------
        EOProduct.validate"""
        return all(key in self for key in self.MANDATORY_FIELD)

    # docstr-coverage: inherited
    def load(self) -> None:
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        if self.store.status == StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open")
        return super().load()

    # docstr-coverage: inherited
    @property
    def name(self) -> str:
        return self._name

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
            self.__set_store(storage=store_or_path_url)
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        self.store.open(mode=mode, **kwargs)
        if mode in ["r"]:
            attributes = self.store[""].attrs
            self.attrs.update(attributes)
            self.set_type(attributes.get(self._TYPE_ATTR_STR, ""))
        return self

    # docstr-coverage: inherited
    @property
    def path(self) -> str:
        return "/"

    # docstr-coverage: inherited
    @property
    def product(self) -> "EOProduct":
        return self

    # docstr-coverage: inherited
    @property
    def relative_path(self) -> Iterable[str]:
        return []

    def set_type(self, type: str, short_names: Optional[dict[str, str]] = None) -> None:
        self.__type = type
        self.attrs[self._TYPE_ATTR_STR] = type
        if short_names is None:
            short_names = dict()
            if type != "":
                mapping = self._mapping_factory.get_mapping(product_type=type)
                for data_mapping in mapping["data_mapping"]:
                    if "short_name" in data_mapping and data_mapping.get("target_path", "") not in ["", "/"]:
                        short_names[data_mapping["short_name"]] = data_mapping["target_path"]
        self.__short_names = short_names

    @property
    def short_names(self) -> dict[str, str]:
        self.type  # check type consistency
        return self.__short_names

    # docstr-coverage: inherited
    @property
    def store(self) -> Optional[EOProductStore]:
        return self._store

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
        for name, group in self.items():
            print(f"├── {name}")
            self._create_structure(group, level=2)  # type: ignore[arg-type]
        return

    @property
    def type(self) -> str:
        if self.__type is not self.attrs[self._TYPE_ATTR_STR]:
            self.set_type(self.attrs[self._TYPE_ATTR_STR])
        return self.__type

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

    # docstr-coverage: inherited
    def write(self) -> None:
        if self.store is None:
            raise StoreNotDefinedError("Store must be defined")
        if self.store.status == StorageStatus.CLOSE:
            raise StoreNotOpenError("Store must be open")
        self.store.write_attrs("", attrs=self.attrs)
        return super().write()

    def _add_local_variable(self, name: str, data: Optional[Any] = None, **kwargs: Any) -> EOVariable:
        raise InvalidProductError("Products can't directly store variables.")

    def _create_structure(self, group: Union[EOGroup, tuple[str, EOGroup]], level: int) -> None:
        if isinstance(group, tuple):
            group = group[1]
        for v in group.variables:
            print("|" + " " * level + "└──", v[0])
        for g in group.groups:
            print("|" + " " * level + "├──", g[0])
            self._create_structure(g, level + 2)

    @property
    def _is_root(self) -> "bool":
        return True

    def _repr_html_(self) -> str:
        from ..formatting import renderer

        return renderer("product.html", product=self)

    def __set_store(self, storage: Optional[Union[str, EOProductStore]] = None) -> None:
        from ..store.zarr import EOZarrStore

        if isinstance(storage, str):
            self._store = EOZarrStore(storage)
        elif isinstance(storage, EOProductStore):
            self._store = storage
        elif storage is not None:
            raise TypeError(f"{type(storage)} can't be used to instantiate EOProductStore.")
