import contextlib
from abc import ABC
from typing import Any

from eopf.computing.abstract import EOProcessingUnit, EOProcessor
from eopf.product.core.eo_product import EOProduct

from .conf.dask_configuration import DaskContext
from .parser.dask_context import EODaskContextParser
from .parser.general import EOProcessParser
from .parser.io import EOIOParser, ModificationMode
from .parser.workflow import EOBreakPointParser, EOTriggerWorkflowParser


class EOTrigger(ABC):
    """Abstract base class provide to implement trigger interface"""

    PARSERS = (EOIOParser(), EODaskContextParser(), EOBreakPointParser(), EOTriggerWorkflowParser())

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
        (processing_unit, io_config, dask_context) = EOTrigger.extract_from_payload(payload)
        inputs_products = io_config["inputs"]
        mode = io_config["file_mode"]
        modif_mode = io_config["modification_mode"]
        with (dask_context, contextlib.ExitStack() as stack):
            products: list[EOProduct] = []
            for input_product in inputs_products:
                product = input_product["instance"].open(mode=mode, **input_product["parameters"])
                stack.enter_context(product)
                products.append(product)

            if isinstance(processing_unit, EOProcessor):
                output = processing_unit.run_validating(*products)
            else:
                output = processing_unit.run(*products)
            if modif_mode == ModificationMode.NEWPRODUCT:
                output_store = io_config["output"]
                stack.enter_context(
                    output.open(mode="w", storage=output_store["instance"], **output_store["parameters"]),
                )
            if modif_mode in [ModificationMode.INPLACE, ModificationMode.NEWPRODUCT]:
                output.write()

    @staticmethod
    def extract_from_payload(
        payload: dict[str, Any],
    ) -> tuple[EOProcessingUnit, dict[str, Any], DaskContext]:
        """Retrieve all the information from the given payload

        the payload should have this keys:

            * 'workflow': describe the processing workflow to run
            * 'breakpoints': configure workflow element as breakpoint
            * 'I/O': configure Input/Ouput element
            * 'dask_context': configure dask scheduler and execution

        See :ref:`triggering-usage`

        Parameters
        ----------
        payload: dict[str, Any]

        Returns
        -------
        tuple:
            All component corresponding to the metadata
        """
        result = EOProcessParser(*EOTrigger.PARSERS).parse(payload)

        dask_context = result["dask_context"]
        io_config = result["I/O"]
        processing_unit = result["workflow"]
        return (
            processing_unit,
            io_config,
            dask_context,
        )
