from typing import Any, Sequence, Union

from numpy.typing import DTypeLike

from eopf.computing.abstract import ProcessingStep, ProcessingUnit, Processor
from eopf.product import EOProduct, EOVariable
from eopf.product.conveniences import merge_product, open_store
from eopf.product.utils import join_eo_path


class ChainedProcessingUnit(ProcessingUnit):
    _PROCESSES: Sequence[ProcessingUnit]

    @property
    def processes(self) -> Sequence[ProcessingUnit]:
        return self._PROCESSES

    def run(self, *args: EOProduct, **kwargs: Any) -> EOProduct:
        products = [product for product in args]
        for unit in self.processes:
            products = [unit(*products, **kwargs)]
        return products[0]


class ChainProcessor(Processor, ChainedProcessingUnit):
    ...


class MergedProcessingUnit(ProcessingUnit):
    _PROCESSES: Sequence[ProcessingUnit]

    @property
    def processes(self) -> Sequence[ProcessingUnit]:
        return self._PROCESSES

    def run(self, *args: EOProduct, **kwargs: Any) -> EOProduct:
        return merge_product(*(process(*args, **kwargs.get(process.identifier, {})) for process in self.processes))


class IdentityProcessingStep(ProcessingStep):
    def apply(  # type: ignore[override]
        self, variable: EOVariable, dtype: DTypeLike = float, **kwargs: Any
    ) -> EOVariable:
        return variable


class IdentityProcessingUnit(ProcessingUnit):
    def run(self, product: EOProduct, **kwargs: Any) -> EOProduct:  # type: ignore[override]
        return product


class ExtractVariableProcessingUnit(ProcessingUnit):

    _VARIABLES_PATHS: Sequence[str] = []
    _CONTAINER_VARIABLES_PATHS: Sequence[str] = []

    @staticmethod
    def _extract_origin_dest_paths(paths: Union[str, tuple[str, str]]) -> tuple[str, ...]:
        if isinstance(paths, str):
            return paths, paths
        return paths

    def run(self, product: EOProduct, **kwargs: Any) -> EOProduct:  # type: ignore[override]
        output = EOProduct("")
        with open_store(product):
            for variable_path in self._VARIABLES_PATHS:
                origin, dest = self._extract_origin_dest_paths(variable_path)
                output[dest] = product[origin]

            for group_path in self._CONTAINER_VARIABLES_PATHS:
                origin, dest = self._extract_origin_dest_paths(group_path)
                for variable_path, variable in product[origin].variables:  # type: ignore[attr-defined]
                    output[join_eo_path(dest, variable_path)] = variable
        return output
