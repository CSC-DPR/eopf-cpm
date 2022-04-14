
How to use Store and Accessors
==============================

Stores and Accessors are inherited from :obj:`eopf.product.store.EOProductStore` and provide a standard way to
access data from an :obj:`eopf.product.EOProduct`

Accessors are specific to legacy product and stores are more generic.


EOSafeStore and Mapping files
-----------------------------

:obj:`eopf.product.store.EOSafeStore` is a kind of store, that aggregate stores and accessor to provide element.
All the definition of the safe is stored in a file, called mapping and follow a specific format.


EOZarrStore
-----------

`Zarr`_ is a format for the storage of chunked, compressed, N-dimensional arrays.
This format is usefull to have parallèle writting over different chunk, to improve efficiency of computation.

the store :obj:`eopf.product.store.EOZarrStore` is a wrapper of this format to help you to use it with our product
format in the eopf framework.

.. _Zarr: https://zarr.readthedocs.io/en/stable/
