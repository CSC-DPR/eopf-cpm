import importlib
from typing import Any, Dict

from eopf.product import EOProduct


class EOQC:
    def __init__(self, check: Dict[str, Any]) -> None:
        self.id = check["check_id"]
        self.version = check["check_version"]
        self.message_if_passed = check["message_if_passed"]
        self.message_if_failed = check["message_if_failed"]
        self.status = False

    def check(self, eoproduct: EOProduct) -> bool:
        return False


class EOQCValidRange(EOQC):
    def __init__(self, check: Dict[str, Any]) -> None:
        super().__init__(check)
        self.eovariable_short_name = check["short_name"]
        self.valid_min = check["valid_min"]
        self.valid_max = check["valid_max"]

    def check(self, eoproduct: EOProduct) -> bool:
        eovariable = eoproduct[eoproduct.short_names[self.eovariable_short_name]]
        if self.valid_min <= eovariable.compute().data.min():
            self.status = eovariable.compute().data.max() <= self.valid_max
        return self.status


class EOQCFormula(EOQC):
    def __init__(self, check: Dict[str, Any]) -> None:
        super().__init__(check)
        self.formula = check["formula"]
        self.thresholds = check["thresholds"]
        self.variables = check["variables_or_attributes"]

    def check(self, eoproduct: EOProduct) -> bool:
        # Test if their is not a rm in formula but security check need to be improve.
        for variable in self.variables:
            if "rm " in variable["name"]:
                return self.status
            if "path" in variable and "rm " in variable["path"]:
                return self.status
            if "formula" in variable and "rm " in variable["formula"]:
                return self.status
        if "rm " in self.formula:
            return self.status
        # Getting and defining variables
        for variable in self.variables:
            if "short_name" in variable:
                locals()[variable["name"]] = eoproduct[eoproduct.short_names[variable["short_name"]]]
            else:
                locals()[variable["name"]] = variable["formula"]
        # Getting and defining thresholds
        for thershold in self.thresholds:
            threshold_name = thershold["name"]
            threshold_value = thershold["value"]
            locals()[threshold_name] = threshold_value
        # Applying the formula
        self.formula = f"self.status = {self.formula}"
        exec(self.formula)  # nosec
        return self.status


class EOQCProcessingUnit(EOQC):
    def __init__(self, check: Dict[str, Any]) -> None:
        super().__init__(check)
        self.module = check["module"]
        self.processing_unit = check["processing_unit"]
        self.parameters = check["parameters"]
        self.aux_data = check["aux_data"]

    def check(self, eoproduct: EOProduct) -> bool:
        module = importlib.import_module(self.module)
        punit_class = getattr(module, self.processing_unit)
        pu = punit_class()
        output = pu.run({"input": eoproduct, "aux_data": self.aux_data}, {"parameters": self.parameters})
        self.status = output.attrs[self.processing_unit]["status"]
        return self.status
