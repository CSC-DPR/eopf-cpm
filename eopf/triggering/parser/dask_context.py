from typing import Any

from eopf.triggering.conf.dask_configuration import DaskContext

from .general import EOTriggeringKeyParser


class EODaskContextParser(EOTriggeringKeyParser):
    """Dask context Parser"""

    KEY = "dask_context"
    OPTIONAL = True
    OPTIONAL_KEYS = ("addr", "cluster_config", "client_config")
    MANDATORY_KEYS = ("cluster_type",)
    DEFAULT = DaskContext()

    def _parse(self, data_to_parse: Any, **kwargs: Any) -> tuple[Any, list[str]]:
        if data_to_parse is None:
            return {}, []
        if not isinstance(data_to_parse, dict):
            raise TypeError(f"dask context misconfigured, should be dict, but is {type(data_to_parse)}")
        errors = self.check_mandatory(data_to_parse) + self.check_unknown(data_to_parse)
        if errors:
            return None, errors
        return DaskContext(**data_to_parse), errors

    def parse(self, data_to_parse: Any, **kwargs: Any) -> Any:
        result = super().parse(data_to_parse, **kwargs)
        if isinstance(result, list) and len(result) > 0:
            return result[0]
        return result
