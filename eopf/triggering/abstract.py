import importlib
from abc import ABC
from typing import Any, Union

import dask

from eopf.computing.abstract import EOProcessingUnit
from eopf.product.core.eo_product import EOProduct
from eopf.product.store.abstract import EOProductStore
from eopf.product.store.store_factory import EOStoreFactory


class EOTrigger(ABC):
    """
    Run Processing unit with its paremeters
    """

    @staticmethod
    def run(
        payload: dict[str, str],
    ) -> None:
        """
        Generic run method

        Scheduler type
        """
        processing_unit, input_product, output_store, scheduler_mode, scheduler_info = EOTrigger.extract_from_payload(
            payload,
        )

        # If dask context asks for local mode we set the local config, else we pass the url of dask scheduler
        if scheduler_mode == "local":
            dask.config.set(scheduler=scheduler_info)
        elif scheduler_mode == "distributed":
            dask.distribution.client(scheduler_info)

        output = processing_unit.run(input_product)
        with output.open(mode="w", store_or_path_url=output_store):
            output.write()

    @staticmethod
    def extract_from_payload(
        payload: dict[str, Any],
    ) -> tuple[EOProcessingUnit, EOProduct, Union[EOProductStore, str], str, str]:
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
        return processing_unit, input_product, ouput_store, scheduler_mode, scheduler_info

    @staticmethod
    def instanciate_store(path: str, store_type: str = "zarr") -> Union[EOProductStore, str]:
        return EOStoreFactory().get_store(path, item_format=store_type)

    @staticmethod
    def instanciate_product(id: str, path: str, store_type: str = "zarr") -> EOProduct:
        store = EOTrigger.instanciate_store(path, store_type=store_type)
        return EOProduct(id, store_or_path_url=store)

    @staticmethod
    def parse_dask_context(dask_context: dict[str, str]) -> tuple[str, str]:
        if "local" in dask_context:
            mode = "local"
        else:  # "distributed" in dask_context
            mode = "distributed"
        scheduler_info = dask_context[mode]
        return mode, scheduler_info

    @staticmethod
    def get_processing_unit(module_name: str, class_name: str) -> EOProcessingUnit:
        if not module_name:
            raise Exception("Missing module name in EOTrigger configuration")
        if not class_name:
            raise Exception("Missing processing unit class name in EOTrigger configuration")
        return getattr(importlib.import_module(module_name), class_name)()
