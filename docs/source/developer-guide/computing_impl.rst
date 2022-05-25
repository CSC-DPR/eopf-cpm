How to implement Computing component
====================================

EOPF-CPM provide abstract classes to help users to write algorithm with EO object (:py:class:`~eopf.product.core.eo_product.EOProduct`, :py:class:`~eopf.product.core.eo_group.EOGroup`, :py:class:`~eopf.product.core.eo_variable.EOVariable`).


Concepts
--------

We introduce distinct concept, step and unit, that are used in different processing level.

* :py:class:`~eopf.computing.abstract.EOProcessingStep`: provide a simple interface to write low level algorithm to work with :py:class:`~dask.array.Array`
* :py:class:`~eopf.computing.abstract.EOProcessingUnit`: are related to product level to create partial and/or complete product.
* :py:class:`~eopf.computing.abstract.EOProcessor`: is a unit that provide a method to validate a product.


Generic Abstract EOProcessingStep
---------------------------------

In addition of this concept and interface, we provide different declination of :py:class:`~eopf.computing.abstract.EOProcessingStep`
for higher granularity.

* :py:class:`~eopf.computing.abstract.EOBlockProcessingStep`
* :py:class:`~eopf.computing.abstract.EOOverlapProcessingStep`

that are direct mapping of :py:func:`~dask.array.map_blocks` (resp. :py:func:`~dask.array.map_overlap`) that let you define only
the low level function passed into the corresponding dask function.


Examples
--------

.. code-block:: python

    class SumProcessStep(EOProcessingStep):
        def apply(self, *args: da.Array, dtype: DTypeLike = float, **kwargs: Any) -> da.Array:
            arg = args[0]
            for a in args[1:]:
                arg += a
            return arg

.. code-block:: python

    class SumProcessingUnit(EOProcessingUnit):
        def run(self, product: EOProduct, **kwargs: Any) -> EOProduct:
            paths = kwargs.get("variables_paths", [])
            dest_path = kwargs.get("dest_path", "/variable")
            step = SumProcessStep()
            new_da = step.apply(*[product[path].data for path in paths])
            new_product = EOProduct("new_product")
            new_product.add_variable(dest_path, data=new_da)
            return new_product

.. code-block:: python

    class SumProcessor(EOProcessor):
        def run(self, product: EOProduct, **kwargs: Any) -> EOProduct:
            with contextlib.ExitStack() as stack:
                if product.store is not None:
                    stack.enter_context(product.open(mode="r"))
                paths = kwargs.get("variables_paths", [])
                dest_path = kwargs.get("dest_path", "/variable")
                step = SumProcessStep()
                new_da = step.apply(*[product[path].data for path in paths])
                new_product = init_product("new_product")
                new_product.add_variable(dest_path, data=new_da)
            return new_product

.. code-block:: python

    class SumBlockProcessingStep(EOBlockProcessingStep):
        def func(self, *args: np.ndarray[Any, np.dtype[Any]], **kwargs: Any) -> np.ndarray[Any, np.dtype[Any]]:
            return sum(args)

.. code-block:: python

    class SumOverlapProcessingStep(EOOverlapProcessingStep):
        def func(self, *args: np.ndarray[Any, np.dtype[Any]], **kwargs: Any) -> np.ndarray[Any, np.dtype[Any]]:
            return sum(args)
