"""The store package provides stores and accessors that allow reading and writing the EOProducts.

All stores and accessors are based on the main abstract EOProductStore class.
"""
from .abstract import EOProductStore, StorageStatus
from .cog import EOCogStore
from .conveniences import convert
from .netcdf import EONetCDFStore
from .safe import EOSafeStore
from .zarr import EOZarrStore

__all__ = [
    "convert",
    "EOZarrStore",
    "EOProductStore",
    "StorageStatus",
    "EONetCDFStore",
    "EOSafeStore",
    "EOCogStore",
]
