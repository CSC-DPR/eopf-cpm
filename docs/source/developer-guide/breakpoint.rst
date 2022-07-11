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
