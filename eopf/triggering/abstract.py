import contextlib
import enum
from abc import ABC
from typing import Any

from eopf.computing.abstract import EOProcessingUnit, EOProcessor
from eopf.product.core.eo_product import EOProduct
from eopf.product.store.abstract import EOProductStore
from eopf.product.store.store_factory import EOStoreFactory

from .dask_configuration import DaskContext
from .workflow import EOProcessorWorkFlow, WorkFlowUnitDescription


class ModificationMode(enum.Enum):
    NEWPRODUCT = "newproduct"
    INPLACE = "inplace"
    READONLY = "readonly"


class EOTrigger(ABC):
    """Abstract base class provide to implement trigger interface"""

    @staticmethod
    def run(
        payload: dict[str, Any],
    ) -> None:
        """Generic method that apply the algortihm of the processing unit
        from the payload and write the result product.

        Parameters
        ----------
        payload: dict[str, Any]
            dict of metadata to find and run the processing unit, create the ouput product
            and write it.
        """
        (processing_unit, io_config, dask_context, parameters) = EOTrigger.extract_from_payload(
            payload,
        )
        inputs_products = io_config["inputs"]
        mode = io_config["file_mode"]
        modif_mode = io_config["modification_mode"]
        with (dask_context, contextlib.ExitStack() as stack):
            products = []
            for input_product in inputs_products:
                product = input_product["product"].open(mode=mode, **input_product["parameters"])
                stack.enter_context(product)
                products.append(product)

            if isinstance(processing_unit, EOProcessor):
                output = processing_unit.run_validating(*products, **parameters)
            else:
                output = processing_unit.run(products, **parameters)
            if modif_mode == ModificationMode.NEWPRODUCT:
                output_store = io_config["output"]
                stack.enter_context(output.open(mode="w", storage=output_store["store"], **output_store["parameters"]))
            if modif_mode in [ModificationMode.INPLACE, ModificationMode.NEWPRODUCT]:
                output.write()

    @staticmethod
    def extract_from_payload(
        payload: dict[str, Any],
    ) -> tuple[EOProcessingUnit, dict[str, Any], DaskContext, dict[str, Any]]:
        """Retrieve all the information from the given payload

        the payload should have this keys:

            * 'module': that provide the module python path
            * 'processing_unit': corresponding to the name of the target processing unit
            * 'parameters': kwargs for the processing unit 'run' method
            * 'I/O':

                - 'modification_mode': one of "newproduct", "inplace", "readonly"
                - 'inputs_products': list of key value pair of data to retrieve input data
                - 'output_product': key value pair of data to find where to write the
                  output product

            * 'dask_context': dask client information

        Examples
        --------
        >>> payload = {
        ...    "module": "tests.computing.test_abstract",
        ...    "processing_unit": "SumProcessor",
        ...    "parameters": {
        ...        "variables_paths": [
        ...            "/measurements/image/oa01_radiance",
        ...            "/measurements/image/oa02_radiance",
        ...        ],
        ...        "dest_path": "/measurements/variable"
        ...    },
        ...    "I/O":{
        ...        "modification_mode": "newproduct",
        ...        "inputs_products": [{
        ...            "id": "OLCI",
        ...            "path": "",
        ...            "store_type": "safe"
        ...        }],
        ...        "output_product": {
        ...            "id": "output",
        ...            "path": "output.zarr",
        ...            "store_type": "zarr"
        ...        }
        ...    },
        ...    "dask_context":{"local": "processes"}
        ...}

        Parameters
        ----------
        payload: dict[str, Any]

        Returns
        -------
        tuple:
            All component corresponding to the metadata
        """
        dask_context = payload.get("dask_context", {})
        ctx = EOTrigger.parse_dask_context(dask_context)
        io_config = EOTrigger.get_io_config(payload["I/O"])
        breakpoints_config = payload.get("breakpoints", {})
        if isinstance((workflow := payload["workflow"]), dict):
            processing_unit = WorkFlowUnitDescription.from_dict(workflow, breakpoints_config)
        else:
            processing_unit = EOProcessorWorkFlow(
                workflow_units=[WorkFlowUnitDescription.from_dict(unit, breakpoints_config) for unit in workflow],
                inputs_name_provided=[p["product"].name for p in io_config["inputs"]],
            )

        return (
            processing_unit,
            io_config,
            ctx,
            payload.get("parameters", {}),
        )

    @staticmethod
    def parse_dask_context(dask_context: dict[str, str]) -> DaskContext:
        """Parse dask context to retrieve cluster and client information

        Parameters
        ----------
        dask_context: dict
            metadata for execution

        Returns
        -------
        DaskContext
        """
        return DaskContext(**dask_context)

    @staticmethod
    def get_io_config(io_config: dict[str, Any]) -> dict[str, Any]:
        loaded_io_config: dict[str, Any] = {}
        modif_mode_mapping = {
            ModificationMode.INPLACE: "r+",
            ModificationMode.READONLY: "r",
            ModificationMode.NEWPRODUCT: "r",
        }
        modification_mode = ModificationMode(io_config.get("modification_mode", "newproduct"))
        loaded_io_config["file_mode"] = modif_mode_mapping[modification_mode]
        loaded_io_config["modification_mode"] = modification_mode
        if modification_mode == ModificationMode.NEWPRODUCT:
            product_info = io_config["output_product"]
            path = product_info.get("path")
            store_type = product_info.get("store_type")
            params = product_info.get("store_params", {})
            output_store = EOTrigger.instanciate_store(path, store_type=store_type)
            loaded_io_config["output"] = {"store": output_store, "parameters": params}

        products = io_config["inputs_products"]
        for product_info in products:
            params = product_info.get("store_params", {})
            input_product = EOTrigger.instanciate_product(
                product_info.get("id", ""),
                product_info["path"],
                product_info.get("store_type", "zarr"),
            )
            loaded_io_config.setdefault("inputs", []).append({"product": input_product, "parameters": params})
        return loaded_io_config

    @staticmethod
    def instanciate_store(path: str, store_type: str = "zarr") -> EOProductStore:
        """Instantiate an EOProductStore from the given inputs

        Parameters
        ----------
        path: str
            path to the corresponding product
        store_type: str
            key for the EOStoreFactory to retrieve the correct type of
            store

        Returns
        -------
        EOProductStore

        See Also
        --------
        eopf.product.store.EOProductStore
        eopf.product.store.store_factory.EOStoreFactory
        """
        return EOStoreFactory().get_store(path, item_format=store_type)

    @staticmethod
    def instanciate_product(id: str, path: str, store_type: str = "zarr") -> EOProduct:
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
        store = EOTrigger.instanciate_store(path, store_type=store_type)
        return EOProduct(id, storage=store)
