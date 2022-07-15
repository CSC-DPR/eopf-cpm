.. _installation:

############
Installation
############

Requirements
============

.. warning::
    To install eopf, you must use:

        * **Python 3.9** or higher
        * **pip 22.0.0** or higher (``$ pip install -U pip`` to update `pip`_)

Virtual Environment
-------------------
You can use any tools you want to manage your virtual environment
(ex: poetry, pipenv, conda, etc ...).

Requirements for conda on Windows
---------------------------------

Windows require a compiled version of **GDAL** for it, but an issue on the **GDAL** python library blocked the installation.
We recommand to install the **rasterio** from conda to provide a fully worked environment::

    $ conda install -c conda-forge rasterio

Requirements for pip install on Windows
---------------------------------------

Windows require a compiled version of **GDAL** for it, but an issue on the **GDAL** python library blocked the installation.
We recommand to download and install with pip the **rasterio** and **GDAL** libraries from the following links:

    * `GDAL`_
    * `rasterio`_


Instruction
===========

To install eopf, after creating your virtual environment, and activate it (ex: ``conda activate`` or ``pipenv shell``),
at your root project level, you can simply use `pip`_ with either a specific Index URL or a GIT URL


Index URL
---------

You can add an index url to help pip to retrieve dependencies by using ``--extra-index-url`` option::

    $ pip install eopf --extra-index-url https://__token__:<your_personal_token>@gitlab.csc-eopf.csgroup.space/api/v4/projects/14/packages/pypi/simple


GIT URL
-------

To make an installation of a python package, `pip`_ manage it by passing the GIT URL prefixed by ``git+``::

    $ pip install git+https://__token__:<your_personal_token>@gitlab.csc-eopf.csgroup.space/cpm/eopf-cpm.git

In this case you can specify a target branch or tag if you want a specific
version by adding ``@targetbranch`` at the end of GIT URL::

    $ pip install git+https://__token__:<your_personal_token>@gitlab.csc-eopf.csgroup.space/cpm/eopf-cpm.git@v0.2.0


Optional dependencies
---------------------

Extra dependencies can be specified from different key

Some keys to work with specific environment:
    * **notebook**: install extra dependencies to use jupyter notebook

Others are used for developpement environment:
    * **tests**: install test dependencies to run pytest test
    * **linter**: install linter to check code style
    * **typing**: install typing tool and library to check code typing
    * **formatter**: install formatter tool to be check code formatting
    * **security**: install tool to analyse security issue
    * **doc**: install library to build the documentation
    * **complexity**: install tool to analyse cyclomatic complexity
    * **doc-cov**: install tool to analyse documentation coverage
    * **cluster-plugin**: install packages for managed dask cluster to run test

for developpers:
    * **dev**: install all optional dependencies and pre-commit

Index URL
~~~~~~~~~

To use optional dependencies with ``extra-index-url`` you just have to add ``[my-keys]`` after the package name:

.. code-block:: bash

    $ pip install eopf[notebook] --extra-index-url https://__token__:<your_personal_token>@gitlab.csc-eopf.csgroup.space/api/v4/projects/14/packages/pypi/simple
    $ pip install eopf[tests,notebook] --extra-index-url https://__token__:<your_personal_token>@gitlab.csc-eopf.csgroup.space/api/v4/projects/14/packages/pypi/simple

GIT URL
~~~~~~~

To use optional dependencies with ``GIT URL`` you just have to add ``#eopf[my-keys]``
at the end of the GIT URL, after the branch name:

.. code-block:: bash

    $ pip install git+https://__token__:<your_personal_token>@gitlab.csc-eopf.csgroup.space/cpm/eopf-cpm.git@v0.2.0#egg=eopf[notebook]
    $ pip install git+https://__token__:<your_personal_token>@gitlab.csc-eopf.csgroup.space/cpm/eopf-cpm.git@v0.2.0#egg=eopf[notebook,tests]


Installation folder
-------------------

Destination of the ``eopf`` package after installation depend of the way that you manage your virtual environment.
please refer to pip and virtual environment management in python.


Uninstall
---------

To uninstall, just use corresponding `pip`_ command::

    $ pip uninstall eopf


Update version
--------------

To update this package, use the previous instruction with the ``update`` option of `pip`_::

    $ pip install -U eopf

.. _GDAL: https://www.lfd.uci.edu/~gohlke/pythonlibs/#gdal
.. _rasterio: https://www.lfd.uci.edu/~gohlke/pythonlibs/#rasterio
.. _pip: https://pip.pypa.io/en/stable/
