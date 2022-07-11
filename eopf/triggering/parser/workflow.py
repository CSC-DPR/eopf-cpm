import importlib
import logging
from dataclasses import dataclass
from typing import Any, Sequence, Union

from eopf.computing import EOProcessingUnit, EOProcessor
from eopf.computing.breakpoint import BreakMode, eopf_class_breakpoint
from eopf.exceptions import EOTriggeringConfigurationError
from eopf.product import EOProduct

from .general import EOTriggeringKeyParser

logger = logging.getLogger("eopf")


class EOBreakPointParser(EOTriggeringKeyParser):
    """breakpoints section Parser"""

    KEY = "breakpoints"
    MANDATORY_KEYS = ("break_mode", "related_unit")
    OPTIONAL_KEYS = ("storage", "store_params")
    OPTIONAL = True
    DEFAULT: dict[str, Any] = {}

    def _parse(self, data_to_parse: Any, **kwargs: Any) -> tuple[Any, list[str]]:
        errors = self.check_mandatory(data_to_parse) + self.check_unknown(data_to_parse)
        if errors:
            return None, errors
        return {
            "related_unit": data_to_parse.get("related_unit", ""),
            "break_mode": BreakMode(data_to_parse.get("break_mode", "r")),
            "storage": data_to_parse.get("storage", ""),
            "store_params": data_to_parse.get("store_params", {}),
        }, errors

    def parse(self, data_to_parse: Any, **kwargs: Any) -> Any:
        result = {}
        for bp in super().parse(data_to_parse, **kwargs):
            result[bp.pop("related_unit")] = bp
        return result


class EOTriggerWorkflowParser(EOTriggeringKeyParser):
    """workflow section Parser"""

    KEY: str = "workflow"
    NEEDS = ["breakpoints"]
    MANDATORY_KEYS = ("module", "processing_unit")
    OPTIONAL_KEYS = ("name", "parameters", "inputs")

    def _parse(
        self,
        data_to_parse: Any,
        breakpoints: dict[str, Any] = {},
        io_config: dict[str, Any] = {},
        **kwargs: Any,
    ) -> tuple[Any, list[str]]:
        errors = self.check_mandatory(data_to_parse) + self.check_unknown(data_to_parse)
        if errors:
            return None, errors
        module_name = data_to_parse.get("module")
        class_name = data_to_parse.get("processing_unit")
        processing_name = data_to_parse.get("name")
        parameters = data_to_parse.get("parameters", {})
        inputs = data_to_parse.get("inputs", [])
        unit_class = getattr(importlib.import_module(module_name), class_name)
        if breakpoint_config := breakpoints.get(processing_name, {}):
            unit_class = eopf_class_breakpoint(unit_class)
            parameters |= breakpoint_config
        unit = unit_class(processing_name)
        processing_unit = WorkFlowUnitDescription(unit, inputs, parameters)
        return processing_unit, errors

    def parse(self, data_to_parse: Union[str, dict[str, Any]], **kwargs: Any) -> Any:
        result = super().parse(data_to_parse, **kwargs)
        if len(result) == 1:
            return result[0]
        return EOProcessorWorkFlow(
            workflow_units=result,
        )


@dataclass
class WorkFlowUnitDescription(EOProcessingUnit):
    """Dataclass use to wrap EOProcessingUnit for triggering execution"""

    processing_unit: EOProcessingUnit
    """Wrapped EOProcessingUnit"""
    inputs: list[str]
    """name of each input to use"""
    parameters: dict[str, Any]
    """all kwargs to use a execution time"""

    @property
    def identifier(self) -> Any:
        return self.processing_unit.identifier

    def run(self, *inputs: EOProduct, **kwargs: Any) -> EOProduct:
        return self.processing_unit.run(*inputs, **self.parameters, **kwargs)


class EOProcessorWorkFlow(EOProcessor):
    """Specific EOProcessor for triggering

    It's used when workflow is a list of processing unit.
    Inputs EOProcessingUnit are sorted at init time to be sure that the
    execution can be done in the correct order.
    """

    def __init__(
        self,
        identifier: Any = "",
        workflow_units: Sequence[WorkFlowUnitDescription] = [],
    ) -> None:
        super().__init__(identifier)
        # reorder units
        products_name = [workflow_unit.identifier for workflow_unit in workflow_units]
        order = []
        while len(products_name) == len(workflow_units):
            for workflow_unit in filter(lambda x: x.identifier in products_name, workflow_units):
                if not any(input_product_name in products_name for input_product_name in workflow_unit.inputs):
                    products_name.remove(workflow_unit.identifier)
                    order.append(workflow_unit.identifier)
            if len(order) == 0 and len(workflow_units) > 0:
                raise EOTriggeringConfigurationError(
                    "workflow miss configured: inputs for unit never match inputs product.",
                )
        self.workflow = sorted(workflow_units, key=lambda x: order.index(x.identifier))

    def run(self, *inputs: EOProduct, **kwargs: Any) -> EOProduct:
        available_products = {product.name: product for product in inputs}
        for unit_description in self.workflow:
            logger.info(f"RUN {unit_description.processing_unit}")
            prod = unit_description.run(*(available_products[prod_name] for prod_name in unit_description.inputs))
            available_products[unit_description.identifier] = prod
        return prod
