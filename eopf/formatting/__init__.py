"""eopf.formatting provide a convenience way to convert object in any eopf workflow.
"""
from .factory import EOFormatterFactory, formatable_func, formatable_method

__all__ = [
    "EOFormatterFactory",
    "formatable_func",
    "formatable_method",
]
