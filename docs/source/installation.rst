################
Installation
################

The getting started guide aims to get you using eopf cpm productively as quickly as possible.
It is designed as an entry point for new users, and it provided an introduction to eopf cpm main concepts.

Installation
============

Requirements
------------

To install eopf, you must use python 3.9 or later

Requirements for conda on Windows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Windows require a compiled version of **GDAL** for it, but an issue on the **GDAL** python library blocked the installation.
We recommand to install the **rasterio** from conda to provide a fully worked environment::

    $ conda install -c conda-forge rasterio

Requirements for pip install on Windows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Windows require a compiled version of **GDAL** for it, but an issue on the **GDAL** python library blocked the installation.
We recommand to download and install with pip the **rasterio** and **GDAL** libraries from the following links:

    * `GDAL`_
    * `rasterio`_

Instruction
-----------

To install eopf, you can simply use pip with the specific index url::

    $ pip install eopf --extra-index-url https://__token__:<your_personal_token>@gitlab.csc-eopf.csgroup.space/api/v4/projects/14/packages/pypi/simple

or using pip with git url::

    $ pip install git+https://__token__:<your_personal_token>@gitlab.csc-eopf.csgroup.space/cpm/eopf-cpm.git

In this case you can specify a target branch or tag if you want a specific version::

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

for developpers:
    * **dev**: install all optional dependencies and pre-commit

Using pip and git
~~~~~~~~~~~~~~~~~

.. code-block:: bash

    $ pip install git+https://__token__:<your_personal_token>@gitlab.csc-eopf.csgroup.space/cpm/eopf-cpm.git@v0.2.0#egg=eopf[notebook]
    $ pip install git+https://__token__:<your_personal_token>@gitlab.csc-eopf.csgroup.space/cpm/eopf-cpm.git@v0.2.0#egg=eopf[notebook,tests]

Using pip and index url
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    $ pip install eopf[notebook] --extra-index-url https://__token__:<your_personal_token>@gitlab.csc-eopf.csgroup.space/api/v4/projects/14/packages/pypi/simple
    $ pip install eopf[tests,notebook] --extra-index-url https://__token__:<your_personal_token>@gitlab.csc-eopf.csgroup.space/api/v4/projects/14/packages/pypi/simple


.. _GDAL: https://www.lfd.uci.edu/~gohlke/pythonlibs/#gdal
.. _rasterio: https://www.lfd.uci.edu/~gohlke/pythonlibs/#rasterio
