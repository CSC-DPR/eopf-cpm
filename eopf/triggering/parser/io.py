import enum
from abc import abstractmethod
from typing import Any

from eopf.product import EOProduct
from eopf.product.store import EOProductStore
from eopf.product.store.store_factory import EOStoreFactory

from .general import EOTriggeringKeyParser


class ModificationMode(enum.Enum):
    NEWPRODUCT = "newproduct"
    INPLACE = "inplace"
    READONLY = "readonly"


class ProductStoreParser(EOTriggeringKeyParser):
    MANDATORY_KEYS = ("id", "path", "store_type")
    OPTIONAL_KEYS = ("store_params",)
    FACTORY = EOStoreFactory()

    def _parse(self, data_to_parse: Any, **kwargs: Any) -> tuple[Any, list[str]]:
        errors = self.check_mandatory(data_to_parse) + self.check_unknown(data_to_parse)
        store_type = data_to_parse.get("store_type")
        if store_type and store_type not in self.FACTORY.item_formats:
            errors.append(f"{store_type=} not recognized, should be one of {tuple(self.FACTORY.item_formats.keys())}.")
        if errors:
            return None, errors
        data_parsed = {
            "instance": self.instanciate(**data_to_parse),
            "parameters": data_to_parse.get("store_params", {}),
        }
        return data_parsed, errors

    def instanciate_store(self, path: str = "", store_type: str = "", **kwargs: Any) -> EOProductStore:
        """Instantiate an EOProductStore from the given inputs

        Parameters
        ----------
        path: str
            path to the corresponding product
        store_type: str
            key for the EOStoreFactory to retrieve the correct type of store

        Returns
        -------
        EOProductStore

        See Also
        --------
        eopf.product.store.EOProductStore
        eopf.product.store.store_factory.EOStoreFactory
        """
        return self.FACTORY.get_store(path, item_format=store_type)

    @abstractmethod
    def instanciate(self, **kwargs: Any) -> Any:
        ...


class EOOutputProductParser(ProductStoreParser):
    """I/O output section Parser"""

    KEY = "output_product"

    def instanciate(self, path: str = "", store_type: str = "", **kwargs: Any) -> Any:
        return self.instanciate_store(path=path, store_type=store_type)

    def parse(self, data_to_parse: Any, **kwargs: Any) -> Any:
        return super().parse(data_to_parse, **kwargs)[0]


class EOInputProductParser(ProductStoreParser):
    """I/O inputs_products section Parser"""

    KEY = "inputs_products"

    def instanciate(self, id: str = "", path: str = "", store_type: str = "", **kwargs: Any) -> Any:
        return self.instanciate_product(id=id, path=path, store_type=store_type)

    def instanciate_product(self, id: str = "", path: str = "", store_type: str = "", **kwargs: Any) -> EOProduct:
        """Instantiate an EOProduct from the given inputs

        Parameters
        ----------
        id: str
            name to give to the product
        path: str
            path to the corresponding product
        store_type: str
            key for the EOStoreFactory to retrieve the correct type of
            store

        Returns
        -------
        EOProduct

        See Also
        --------
        eopf.product.EOProduct
        """
        store = self.instanciate_store(path, store_type=store_type)
        return EOProduct(id, storage=store)


class EOIOParser(EOTriggeringKeyParser):
    """I/O section Parser"""

    KEY = "I/O"
    MANDATORY_KEYS = ("inputs_products",)
    OPTIONAL_KEYS = ("output_product", "modification_mode")

    @property
    def modif_mode_mapping(self) -> dict[ModificationMode, str]:
        return {
            ModificationMode.INPLACE: "r+",
            ModificationMode.READONLY: "r",
            ModificationMode.NEWPRODUCT: "r",
        }

    def _parse(self, data_to_parse: Any, **kwargs: Any) -> Any:
        errors = self.check_mandatory(data_to_parse) + self.check_unknown(data_to_parse)
        if errors:
            return None, errors
        loaded_io_config: dict[str, Any] = {}
        modification_mode = ModificationMode(data_to_parse.get("modification_mode", ModificationMode.NEWPRODUCT.value))
        loaded_io_config["file_mode"] = self.modif_mode_mapping[modification_mode]
        loaded_io_config["modification_mode"] = modification_mode
        if modification_mode == ModificationMode.NEWPRODUCT:
            loaded_io_config["output"] = EOOutputProductParser().parse(data_to_parse)
        loaded_io_config["inputs"] = EOInputProductParser().parse(data_to_parse)
        return loaded_io_config, errors

    def parse(self, data_to_parse: Any, **kwargs: Any) -> Any:
        return super().parse(data_to_parse, **kwargs)[0]
