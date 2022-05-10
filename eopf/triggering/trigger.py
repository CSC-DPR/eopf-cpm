from abc import ABC

import dask

from eopf.computing.abstract import EOProcessingUnit
from eopf.product.core.eo_product import EOProduct
from eopf.product.store.abstract import EOProductStore


class Trigger(ABC):
    """
    Run Processing unit <ith its paremeters
    """

    def run(
        input_product: EOProduct,
        output_store: EOProductStore,
        processing_unit: EOProcessingUnit,
        dask_context: dict = {},
    ):
        """
        Generic run method
        """

        # If dask context asks for local mode we set the local config, else we pass the url of dask scheduler
        if "local" in dask_context:
            dask.config.set(scheduler=dask_context["local"])
        elif "distributed" in dask_context:
            dask.distribution.client(dask_context["distributed"])

        output = processing_unit(input_product)
        with output.open(mode="w", store_or_path_url=output_store):
            ...
