import importlib
import logging
from dataclasses import dataclass
from typing import Any, Sequence

from eopf.computing import EOProcessingUnit, EOProcessor
from eopf.computing.breakpoint import BreakMode, eopf_class_breakpoint
from eopf.product import EOProduct

logger = logging.getLogger("eopf")


@dataclass
class WorkFlowUnitDescription(EOProcessingUnit):
    processing_unit: EOProcessingUnit
    inputs: Sequence[str]
    ouput: str
    parameters: dict[str, Any]
    config: dict[str, Any]

    @property
    def identifier(self) -> Any:
        return self.processing_unit.identifier

    @classmethod
    def from_dict(cls, description: dict[str, Any], breakpoints_config: dict[str, Any] = {}):
        def map_breakpoint_config(context: dict[str, Any]) -> dict[str, Any]:
            return {
                "break_mode": BreakMode(context.get("break_mode", "r")),
                "storage": context.get("storage", ""),
                "store_params": context.get("store_params", {}),
            }

        if not (module_name := description.get("module")):
            raise AttributeError("Missing module name in EOTrigger configuration")
        if not (class_name := description.get("processing_unit")):
            raise AttributeError("Missing processing unit class name in EOTrigger configuration")
        processing_name = description.get("name")
        unit = getattr(importlib.import_module(module_name), class_name)(processing_name)
        if breakpoint_config := breakpoints_config.get(unit.identifier, {}):
            unit = eopf_class_breakpoint(unit)
            breakpoint_config = map_breakpoint_config(breakpoint_config)

        if not isinstance(unit, EOProcessingUnit):
            raise TypeError(
                f"Processing unit given in {module_name}.{class_name} must inherited from eopf.computing.EOProcessing",
            )
        return cls(
            unit,
            description.get("inputs", []),
            description.get("output"),
            description.get("parameters"),
            breakpoint_config,
        )

    def run(self, *inputs: EOProduct, context: dict[str, Any] = {}, **kwargs: Any) -> EOProduct:
        return self.processing_unit.run(*inputs, context=self.config | context, **self.config, **kwargs)


class EOProcessorWorkFlow(EOProcessor):
    def __init__(
        self,
        identifier: Any = "",
        workflow_units: Sequence[WorkFlowUnitDescription] = [],
        inputs_name_provided: list[str] = [],
    ) -> None:
        super().__init__(identifier)
        # products_name = [i for i in inputs_name_provided]
        # ids = []
        # while not len(products_name) != len(inputs_name_provided) + len(workflow_units):
        #     for idx, workflow_unit in filter(lambda x: x.identifier not in products_name, enumerate(workflow_units)):
        #         if all(input_product_name in products_name for input_product_name in workflow_unit.inputs):
        #             ids.append(idx)
        #             products_name.append(workflow_unit.identifier)
        # self.workflow = sorted(workflow_units, key=lambda x: ids.index(workflow_units.index(x)))
        self.workflow = workflow_units

    def run(self, *inputs: EOProduct, context: dict[str, Any] = {}, **kwargs: Any) -> EOProduct:
        available_products = {product.name: product for product in inputs}
        for unit_description in self.workflow:
            logger.info(f"RUN {unit_description.processing_unit}")
            prod = unit_description.run(*(available_products[prod_name] for prod_name in unit_description.inputs))
            available_products[unit_description.identifier] = prod
        return prod
