import importlib
from abc import ABC
from typing import Any, Union

import dask

from eopf.computing.abstract import EOProcessingUnit, EOProcessor
from eopf.product.core.eo_product import EOProduct
from eopf.product.store.abstract import EOProductStore
from eopf.product.store.store_factory import EOStoreFactory


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
        (
            processing_unit,
            input_product,
            output_store,
            scheduler_mode,
            scheduler_info,
            parameters,
            input_product_parameter,
            output_product_parameter,
        ) = EOTrigger.extract_from_payload(
            payload,
        )

        dask.config.set(scheduler=scheduler_info)
        with input_product.open(mode="r", **input_product_parameter):
            if isinstance(processing_unit, EOProcessor):
                output = processing_unit.run_validating(input_product, **parameters)
            else:
                output = processing_unit.run(input_product, **parameters)
            with output.open(mode="w", storage=output_store, **output_product_parameter):
                output.write()

    @staticmethod
    def extract_from_payload(
        payload: dict[str, Any],
    ) -> tuple[
        EOProcessingUnit,
        EOProduct,
        Union[EOProductStore, str],
        str,
        str,
        dict[str, Any],
        dict[str, Any],
        dict[str, Any],
    ]:
        """Retrieve all the information from the given payload

        the payload should have this keys:

            * 'module': that provide the module python path
            * 'processing_unit': corresponding to the name of the target processing unit
            * 'parameters': kwargs for the processing unit 'run' method
            * 'input_product': key value pair of data to retrieve input data
            * 'output_product': key value pair of data to find where to write the
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
        ...    "input_product": {
        ...        "id": "OLCI",
        ...        "path": "",
        ...        "store_type": "safe"
        ...    },
        ...    "output_product": {
        ...        "id": "output",
        ...        "path": "output.zarr",
        ...        "store_type": "zarr"
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

        input_product_payload = payload.get("input_product", {})
        output_product_payload = payload.get("output_product", {})
        dask_context = payload.get("dask_context", {})
        processing_unit = EOTrigger.get_processing_unit(payload["module"], payload["processing_unit"])
        input_product = EOTrigger.instanciate_product(
            input_product_payload.get("id"),
            input_product_payload.get("path"),
            input_product_payload.get("store_type", "zarr"),
        )
        ouput_store = EOTrigger.instanciate_store(
            output_product_payload.get("path"),
            output_product_payload.get("store_type", "zarr"),
        )
        scheduler_mode, scheduler_info = EOTrigger.parse_dask_context(dask_context)
        return (
            processing_unit,
            input_product,
            ouput_store,
            scheduler_mode,
            scheduler_info,
            payload.get("parameters", {}),
            input_product_payload.get("parameters", {}),
            output_product_payload.get("parameters", {}),
        )

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

    @staticmethod
    def parse_dask_context(dask_context: dict[str, str]) -> tuple[str, str]:
        """Parse dask context to retrieve scheduler info

        Parameters
        ----------
        dask_context: dict
            metadata of the scheduler information

        Returns
        -------
        mode: str
            local of distributed
        scheduler_info: str
            scheduler value to configure dask
        """
        if "local" in dask_context:
            mode = "local"
        else:  # "distributed" in dask_context
            mode = "distributed"
        scheduler_info = dask_context[mode]
        return mode, scheduler_info

    @staticmethod
    def get_processing_unit(module_name: str, class_name: str) -> EOProcessingUnit:
        """Retrieve a processing unit from module and class name

        Parameters
        ----------
        module_name: str
            name for the module
        class_name: str
            name of the processing unit class

        Returns
        -------
        EOProcessingUnit
        """
        if not module_name:
            raise AttributeError("Missing module name in EOTrigger configuration")
        if not class_name:
            raise AttributeError("Missing processing unit class name in EOTrigger configuration")
        return getattr(importlib.import_module(module_name), class_name)()
