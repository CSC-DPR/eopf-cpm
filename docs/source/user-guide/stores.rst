
How to use Store and Accessors
==============================

Stores and Accessors are inherited from :py:class:`~eopf.product.store.abstract.EOProductStore` and provide a standard way to
access data from an :py:class:`~eopf.product.core.eo_product.EOProduct`

Accessors are specific to legacy product and stores are more generic.


EOSafeStore and Mapping files
-----------------------------

:py:class:`~eopf.product.store.safe.EOSafeStore` is a kind of store, that aggregate stores and accessor to provide element.
All the definition of the safe is stored in a file, called mapping and follow a specific format.


EOZarrStore
-----------

`Zarr`_ is a format for the storage of chunked, compressed, N-dimensional arrays.
This format is usefull to have parallèle writting over different chunk, to improve efficiency of computation.

the store :py:class:`~eopf.product.store.zarr.EOZarrStore` is a wrapper of this format to help you to use it with our product
format in the eopf framework.

.. _Zarr: https://zarr.readthedocs.io/en/stable/
