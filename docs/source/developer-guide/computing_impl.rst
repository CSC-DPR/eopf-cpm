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


.. _breakpoint-usage:

Intermediate Output - BreakPoint Object
=======================================

Sometime, when you implement a functionality or a processor, you want to produce some intermediate output to help you in
debugging or to facilitate to next execution.

To help you in this way, we provide :py:func:`~eopf.computing.breakpoint.eopf_breakpoint` decorator,
and :py:func:`~eopf.computing.breakpoint.eopf_class_breakpoint` decorator, that can be used to wrap function (resp. class) to manage
breakpoint mode.

this wrappers add some parameters to your wrapped elements to manage them as follow:

break_mode:
    a parameter use to establish what you need to do

    * :py:obj:`BreakMode.RETRIEVE` to try to find an already writted element
    * :py:obj:`BreakMode.FORCE_WRITE` to write your element, even if one already exist
    * :py:obj:`BreakMode.SKIP` to ignore breakpoint wrapper and just execute your function (default)

storage:
    uri to the element to retrieve or write

store_params:
    a dictionary of element to give to the `open` method used.


.. code-block:: python

    from eopf.computing.breakpoint import eopf_breakpoint, BreakMode

    @eopf_breakpoint(allowed_mode=[BreakMode.RETRIEVE, BreakMode.FORCE_WRITE])
    def my_function(eoproduct: EOProduct) -> EOProduct:
        new_eo_product = EOProduct("new_one")
        new_eo_product.add_variable("measurements/a_variable", eoproduct["measurements/image/oa10_radiance"])
        new_eo_product.add_variable("measurements/an_other_variable", data=eoproduct["measurements/orphans/oa10_radiance"])
        return eo_product

.. code-block:: python

    from eopf.computing.breakpoint import eopf_class_breakpoint, BreakMode
    from eopf.computing import EOProcessor

    @eopf_class_breakpoint(allowed_mode=[BreakMode.RETRIEVE, BreakMode.FORCE_WRITE], methods=["run"])
    class MyProcessor(eoproduct: EOProduct):

        def run(eoproduct: EOProduct) -> EOProduct:
            new_eo_product = EOProduct("new_one")
            new_eo_product.add_variable("measurements/a_variable", eoproduct["measurements/image/oa10_radiance"])
            new_eo_product.add_variable("measurements/an_other_variable", data=eoproduct["measurements/orphans/oa10_radiance"])
            return eo_product
