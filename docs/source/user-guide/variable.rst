.. _eovariable-usage:

Using Variable
==============

Operation
---------

:py:class:`~eopf.product.core.eo_variable.EOVariable` can be used for computation and support base operator:

    .. jupyter-execute::

        from eopf.product import EOVariable
        import numpy as np

        variable = EOVariable(data=np.random.random_sample(size=(128,128)))
        variable *= 2
        variable += variable
        variable = variable / 1.2
        variable **= 2
        variable = variable - (variable / 2)
        variable

In this case, the variable is not computed, to apply operation, you have to use the method :py:meth:`~eopf.product.core.eo_variable.EOVariable.compute`

    .. jupyter-execute::

        variable.compute()


The data of a variable is accessible direcly with the :py:attr:`~eopf.product.core.eo_variable.EOVariable.data` property:

    .. jupyter-execute::

        variable.data


You can also create mask variable using comparison:

    .. jupyter-execute::

        mask = (variable > 0.5).compute().data
        mask

    .. jupyter-execute::

        mask = (variable > ((variable > 0.5) * 2)).compute().data
        mask

    And apply a mask on an existing variable:

    .. jupyter-execute::

        variable[mask[1,:]].compute().data


Chunking of EOVariable
----------------------

As :py:class:`xarray.DataArray`, we provide some convenience way to work with chunks over EOVariable.

You can retrieve the chunking of :py:class:`~eopf.product.core.eo_variable.EOVariable`
with the property :py:attr:`~eopf.product.core.eo_variable.EOVariable.chunks`

    .. jupyter-execute::

        variable.chunks

you also can set the chunking with :py:meth:`~eopf.product.core.eo_variable.EOVariable.chunk`

    .. jupyter-execute::

        variable.chunk((2,))


Plotting an EOVariable
-----------------------

    :py:class:`~eopf.product.core.eo_variable.EOVariable` provide a :py:meth:`~eopf.product.core.eo_variable.EOVariable.plot` methode, to plot them, it use :py:mod:`~matplotlib.pyplot`:

    .. jupyter-execute::

        import matplotlib.pyplot as plt

        (variable*256).plot()
        plt.ylabel("y")
        plt.xlabel("x")
        plt.xlim(0, 128)
        plt.ylim(0, 128)
        plt.draw()
