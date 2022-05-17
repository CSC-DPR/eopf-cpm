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
        (
            processing_unit,
            input_product,
            output_store,
            scheduler_mode,
            scheduler_info,
            parameters,
        ) = EOTrigger.extract_from_payload(
            payload,
        )

        dask.config.set(scheduler=scheduler_info)
        output = processing_unit.run(input_product, **parameters)
        with output.open(mode="w", store_or_path_url=output_store):
            output.write()

    @staticmethod
    def extract_from_payload(
        payload: dict[str, Any],
    ) -> tuple[EOProcessingUnit, EOProduct, Union[EOProductStore, str], str, str, dict[str, Any]]:
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
        parameters = payload.get("parameters", {})
        return processing_unit, input_product, ouput_store, scheduler_mode, scheduler_info, parameters

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
            raise AttributeError("Missing module name in EOTrigger configuration")
        if not class_name:
            raise AttributeError("Missing processing unit class name in EOTrigger configuration")
        return getattr(importlib.import_module(module_name), class_name)()
