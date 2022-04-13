Using Variable for computing
============================

:obj:`eopf.product.EOVariable` can be used for computation and support base operator:

    .. jupyter-execute::

        from eopf.product import EOVariable

        variable = EOVariable(data=[1,2,3])
        variable *= 2
        variable += variable
        variable = variable / 10
        variable **= 2
        variable = variable - (variable / 2)
        variable

In this case, the variable is not computed, to apply operation, you have to use the method :obj:`EOVariable.compute()`

    .. jupyter-execute::

        variable.compute()

The data of a variable is accessible direcly with the `data` property:

    .. jupyter-execute::

        variable.data
