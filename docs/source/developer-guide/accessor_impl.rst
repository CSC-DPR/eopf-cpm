How to implement a Store/Accessor
=================================

For reading unsuported file format, or apply complex (*check SAFE store parameters first*) you might want to implement your own Store/Accessor.

Difference between a Store and an Accessor
------------------------------------------

The only difference between a Store and an Accessor is it's main usage :

- Store are expected to be used independently to open a file. Stores can always be used as Accessor.
- Accessor are expected to be used in the SAFE store to open a file. Some can be used as stores too.

Accessors can be used as Store but might not provide a representation compatible with EO standard (variable in top level group), have weird key or be iterable on all their hierarchy.

While it's recommended to implement all accessor as if they were store, it's not recomanded to open product directly with accessors.

In the rest of the document Accessor ans Store wil be uses interchangeably as they ideally should be both.


Interfaces and implementation
-----------------------------

All accessors/stores must implement the EOProductStore interface. For read only stores you can instead implement the EOReadOnlyStore.

See their documentation for more details : :obj:`eopf.product.core.eo_abstract.EOAbstract`


Accessor : correspondence with SAFE json mapping
------------------------------------------------

accessor_config
    passed to the accessor in the open method kwargs
local_path
    the path/key of most methods.
parameters
    **NOT** managed by the accessor, they are transformations applied to the gotten items. Should be used in preference for simple/available transformation (eg : changing dimensions name). Note that it's possible for the user to add additional transformation types.

Use your store (without modifying the eopf sources)
---------------------------------------------------
For using it as a store
    Initialise your store then initialise a product from it.

For using it as an accessor
    Create a EOStoreFactory, register your accessor to it and initialise your SafeStore with it in it's store_factory. Cf :obj:`eopf.product.store.store_factory.EOStoreFactory`
