"""eopf.product module provide to the developers of the re-engineered processors
a homogenous access interface to Copernicus products.

This module allows loading, updating and storing in parallel Copernicus products from/to multiple storage systems
(S3, NFS, POSIX …) and using different formats (Zarr, NetCDF4(HDF5), Cloud optimized GeoTIFF).

The eopf.product module also provides an opportunistic caching mechanism
used to store intermediary results into L0 products.
This cache is used to store computing results so that they can be reused
if a product is reprocessed. The most interesting calculation results to cache are those
that have a low probability of change in case of reprocessing, are time consuming to produce and are small in size.

The eopf.product module is composed mainly by the following two sub-packages:

    * eopf.product.core
    * eopf.product.store
"""
from .conveniences import open_store
from .core import EOGroup, EOProduct, EOVariable

__all__ = ["EOGroup", "EOProduct", "EOVariable", "open_store"]
