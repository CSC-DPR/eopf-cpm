import warnings
from typing import Any, Callable, Sequence, Union

from numpy.typing import DTypeLike

from eopf.computing.abstract import EOProcessingStep, EOProcessingUnit, EOProcessor
from eopf.logging import logger
from eopf.product import EOProduct, EOVariable
from eopf.product.conveniences import merge_product
from eopf.product.utils import join_eo_path


class EOChainedProcessingUnit(EOProcessingUnit):
    _PROCESSES: Sequence[EOProcessingUnit]

    @property
    def processes(self) -> Sequence[EOProcessingUnit]:
        return self._PROCESSES

    def run(self, *args: EOProduct, **kwargs: Any) -> EOProduct:
        products = [product for product in args]
        for unit in self.processes:
            logger.info(f"{unit}")
            products = [unit(*products, **kwargs)]
        return products[0]


class EOChainProcessor(EOProcessor, EOChainedProcessingUnit):
    ...


class EOMergedProcessingUnit(EOProcessingUnit):
    _PROCESSES: Sequence[EOProcessingUnit]

    @property
    def processes(self) -> Sequence[EOProcessingUnit]:
        return self._PROCESSES

    def run(self, *args: EOProduct, **kwargs: Any) -> EOProduct:
        return merge_product(*(process(*args, **kwargs.get(process.identifier, {})) for process in self.processes))


class EOIdentityProcessingStep(EOProcessingStep):
    def apply(  # type: ignore[override]
        self, variable: EOVariable, dtype: DTypeLike = float, **kwargs: Any
    ) -> EOVariable:
        return variable


class EOIdentityProcessingUnit(EOProcessingUnit):
    def run(self, product: EOProduct, **kwargs: Any) -> EOProduct:  # type: ignore[override]
        return product


class EOExtractVariableProcessingUnit(EOProcessingUnit):

    _VARIABLES_PATHS: Sequence[str] = []
    _CONTAINER_VARIABLES_PATHS: Sequence[str] = []

    @staticmethod
    def _extract_origin_dest_paths(paths: Union[str, tuple[str, str]]) -> tuple[str, ...]:
        if isinstance(paths, str):
            return paths, paths
        return paths

    def run(self, product: EOProduct, **kwargs: Any) -> EOProduct:  # type: ignore[override]
        output = self.instantiate_output()
        for variable_path in self._VARIABLES_PATHS:
            origin, dest = self._extract_origin_dest_paths(variable_path)
            output[dest] = product[origin]

        for group_path in self._CONTAINER_VARIABLES_PATHS:
            origin, dest = self._extract_origin_dest_paths(group_path)
            for variable_path, variable in product[origin].variables:  # type: ignore[attr-defined]
                output[join_eo_path(dest, variable_path)] = variable
        return output


try:

    from prefect import Flow, Parameter, task

    def migrate_unit_to_prefect(unit: EOProcessingUnit, *task_args: Any, **task_kwargs: Any) -> Callable:
        @task(*task_args, **task_kwargs)
        def dec(args, kwargs) -> EOProduct:
            return unit(*args, **kwargs)

        return dec

    def define_prefect_workflow(processor: EOProcessor, workflow_name="") -> Flow:
        workflow_name = workflow_name or f"Processor<{processor.identifier}>"
        with Flow(workflow_name) as flow:
            args = Parameter("args", default=[])
            kwargs = Parameter("kwargs", default={})
            migrate_unit_to_prefect(processor, name=workflow_name, log_stdout=True)(args, kwargs)
        return flow


except ModuleNotFoundError:
    warnings.warn("If you want to use prefect please install extra dependencies 'eopf[prefect]'")
