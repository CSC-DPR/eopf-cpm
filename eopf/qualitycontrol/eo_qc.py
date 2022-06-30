import importlib
import itertools
from abc import ABC, abstractmethod

from eopf.product import EOProduct
from eopf.product.core.eo_variable import EOVariable


class EOQC(ABC):
    """Quality check class.

    Parameters
    ----------
    id: str
        The id of the quality check.
    version: str
        The version of the quality check.
    message_if_passed: str
        The message if the quality check pass.
    message_if_failed: str
        The message if the quality check fail.

    Attributes
    ----------
    id: str
        The id of the quality check.
    version: str
        The version of the quality check.
    message_if_passed: str
        The message if the quality check pass.
    message_if_failed: str
        The message if the quality check fail.
    _status: bool
        Status of the quality check, true if it's ok, false if not ot if the quality check was not executed.

    """

    def __init__(self, check_id: str, check_version: str, message_if_passed: str, message_if_failed: str) -> None:
        self.id = check_id
        self.version = check_version
        self.message_if_passed = message_if_passed
        self.message_if_failed = message_if_failed
        self._status = False

    @abstractmethod
    def check(self, eoproduct: EOProduct) -> bool:  # pragma: no cover
        """Check method for a quality check.

        Parameters
        ----------
        eoproduct: EOProduct
            The product to check.

        Returns
        -------
        bool
            Status of the quality check, true if it's ok, false if not.
        """
        return False

    @property
    def status(self) -> bool:
        """The status of the quality check"""
        return bool(self._status)


class EOQCValidRange(EOQC):
    """Quality valid range check class.

    Parameters
    ----------
    id: str
        The id of the quality check.
    version: str
        The version of the quality check.
    message_if_passed: str
        The message if the quality check pass.
    message_if_failed: str
        The message if the quality check fail.
    eovariable_short_name: str
        Short name  of the variable in the product.
    valid_min: float
        The minimium value of the range.
    valid_max: float
        The maximum value of the range.

    Attributes
    ----------
    id: str
        The id of the quality check.
    version: str
        The version of the quality check.
    message_if_passed: str
        The message if the quality check pass.
    message_if_failed: str
        The message if the quality check fail.
    _status: bool
        Status of the quality check, true if it's ok, false if not ot if the quality check was not executed.
    eovariable_short_name: str
        Short name  of the variable in the product.
    valid_min: float
        The minimium value of the range.
    valid_max: float
        The maximum value of the range.
    """

    def __init__(
        self,
        check_id: str,
        check_version: str,
        message_if_passed: str,
        message_if_failed: str,
        short_name: str,
        valid_min: float,
        valid_max: float,
    ) -> None:
        super().__init__(check_id, check_version, message_if_passed, message_if_failed)
        self.eovariable_short_name = short_name
        self.valid_min = valid_min
        self.valid_max = valid_max

    # docstr-coverage: inherited
    def check(self, eoproduct: EOProduct) -> bool:
        eovariable: EOVariable = eoproduct[self.eovariable_short_name]  # type: ignore[assignment]
        variable = eovariable.compute()
        if self.valid_min <= variable.data.min():
            self._status = variable.data.max() <= self.valid_max
        return self.status


class EOQCFormula(EOQC):
    """Quality formula check class.

    Parameters
    ----------
    id: str
        The id of the quality check.
    version: str
        The version of the quality check.
    message_if_passed: str
        The message if the quality check pass.
    message_if_failed: str
        The message if the quality check fail.
    formula: str
        Formula to execute.
    thresholds: dict[str, Any]
        The different thresholds use in the formula.
    variables: dict[str, Any]
        The different variables use in the formula.

    Attributes
    ----------
    id: str
        The id of the quality check.
    version: str
        The version of the quality check.
    message_if_passed: str
        The message if the quality check pass.
    message_if_failed: str
        The message if the quality check fail.
    _status: bool
        Status of the quality check, true if it's ok, false if not ot if the quality check was not executed.
    formula: str
        Formula to execute.
    thresholds: dict[str, Any]
        The different thresholds use in the formula.
    variables: dict[str, Any]
        The different variables use in the formula.
    """

    SECURITY_TOKEN = ["rm"]

    def __init__(
        self,
        check_id: str,
        check_version: str,
        message_if_passed: str,
        message_if_failed: str,
        formula: str,
        thresholds: dict,
        variables_or_attributes: dict,
    ) -> None:
        super().__init__(check_id, check_version, message_if_passed, message_if_failed)
        self.formula = formula
        self.thresholds = thresholds
        self.variables = variables_or_attributes

    # docstr-coverage: inherited
    def check(self, eoproduct: EOProduct) -> bool:
        # Test if their is not a rm in formula but security check need to be improve.
        iterator = itertools.product(self.SECURITY_TOKEN, self.variables, ["name", "path", "formula"])
        if any(token in variable.get(var_item, "") for token, variable, var_item in iterator):
            return self.status
        # Getting and defining variables
        local_var = locals()
        for variable in self.variables:
            local_var[variable["name"]] = eoproduct[variable["short_name"]]
        # Getting and defining thresholds
        for thershold in self.thresholds:
            threshold_name = thershold["name"]
            threshold_value = thershold["value"]
            local_var[threshold_name] = threshold_value
        # Applying the formula
        self._status = eval(f"{self.formula}")  # nosec
        return self.status


class EOQCProcessingUnit(EOQC):
    """Quality processing unit check class.

    Parameters
    ----------
    id: str
        The id of the quality check.
    version: str
        The version of the quality check.
    message_if_passed: str
        The message if the quality check pass.
    message_if_failed: str
        The message if the quality check fail.
    module: str
        Path to the module to execute.
    processing_unit: str
        Processing unit to be executed int the module.
    parameters: dict[str, Any]
        Parameters use by the processing unit.
    aux_data: dict[str, Any]
        Data use by the processing unit.

    Attributes
    ----------
    id: str
        The id of the quality check.
    version: str
        The version of the quality check.
    message_if_passed: str
        The message if the quality check pass.
    message_if_failed: str
        The message if the quality check fail.
    _status: bool
        Status of the quality check, true if it's ok, false if not ot if the quality check was not executed.
    module: str
        Path to the module to execute.
    processing_unit: str
        Processing unit to be executed int the module.
    parameters: dict[str, Any]
        Parameters use by the processing unit.
    aux_data: dict[str, Any]
        Data use by the processing unit.
    """

    def __init__(
        self,
        check_id: str,
        check_version: str,
        message_if_passed: str,
        message_if_failed: str,
        module: str,
        processing_unit: str,
        parameters: dict,
        aux_data: dict,
    ) -> None:
        super().__init__(check_id, check_version, message_if_passed, message_if_failed)
        self.module = module
        self.processing_unit = processing_unit
        self.parameters = parameters
        self.aux_data = aux_data

    # docstr-coverage: inherited
    def check(self, eoproduct: EOProduct) -> bool:
        module = importlib.import_module(self.module)
        punit_class = getattr(module, self.processing_unit)
        pu = punit_class()
        output = pu.run({"input": eoproduct, "aux_data": self.aux_data}, {"parameters": self.parameters})
        self._status = output.attrs[self.processing_unit]["status"]
        return self.status
