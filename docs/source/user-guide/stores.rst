
How to use Store and Accessors
==============================

Stores and Accessors are inherited from :obj:`eopf.product.store.EOProductStore` and provide a standard way to
access data from an :obj:`eopf.product.EOProduct`

Accessors are specific to legacy product and stores are more generic.


EOSafeStore and Mapping files
-----------------------------

:obj:`eopf.product.store.EOSafeStore` is a kind of store, that aggregate stores and accessor to provide element.
All the definition of the safe is stored in a file, called mapping and follow a specific format.
