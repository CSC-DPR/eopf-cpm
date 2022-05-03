from abc import ABC
from typing import Any, Sequence

from numpy import _DTypeLike

from eopf.computing.abstract import ProcessingStep, ProcessingUnit, Processor
from eopf.product import EOProduct, EOVariable
from eopf.product.conveniences import merge_product, open_store


class ChainedMixin(ABC):
    _PROCESSES: Sequence[Any]

    @property
    def processes(self):
        return self._PROCESSES

    def run(self, *args: EOProduct, **kwargs: Any) -> EOProduct:
        products = [product for product in args]
        for unit in self.processes:
            products = [unit(*products, **kwargs)]
        return products[0]


class ChainedProcessingStep(ProcessingUnit, ChainedMixin):
    _PROCESSES: Sequence[ProcessingStep]


class ChainedProcessingUnit(ProcessingUnit, ChainedMixin):
    _PROCESSES: Sequence[ProcessingUnit]


class ChainProcessor(Processor, ChainedProcessingUnit):
    ...


class MergedProcessingUnit(ProcessingUnit):
    _PROCESSES: Sequence[ProcessingUnit]

    @property
    def processes(self):
        return self._PROCESSES

    def run(self, *args: EOProduct, **kwargs: Any) -> EOProduct:
        return merge_product(*(process(*args, **kwargs.get(process.identifier, {})) for process in self.processes))


class MergedProcessingStep(ProcessingUnit):
    _PROCESSES: Sequence[ProcessingUnit]

    @property
    def processes(self):
        return self._PROCESSES

    def run(self, product: EOProduct, **kwargs: Any) -> EOProduct:
        return merge_product(
            *(
                process(product[kwargs[process.identifier]["variable_path"]], **kwargs[process.identifier])
                for process in self.processes
            )
        )


class IdentityProcessingStep(ProcessingStep):
    def apply(self, variable: EOVariable, dtype: _DTypeLike = float, **kwargs: Any) -> EOVariable:
        return variable


class IdentityProcessingUnit(ProcessingUnit):
    def run(self, product: EOProduct, **kwargs: Any) -> EOProduct:
        return product


class ExtractVariableProcessingUnit(ProcessingUnit):

    _VARIABLE_PATH: str

    def run(self, *args: EOProduct, **kwargs: Any) -> EOProduct:
        product = args[0]
        with open_store(product):
            ...
