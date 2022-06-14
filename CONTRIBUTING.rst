Install your environment
========================

The current target python version is **3.9**.
You can use any tools you want to manage your virtual environment (ex: poetry, pipenv, conda, etc ...).

Install eopf as development package
-----------------------------------

After creating your virtual environment, and activate it (ex: ``conda activate`` or ``pipenv shell``),
at the root level of the eopf-cpm project, run the following installation command::

    $ pip install -e ".[dev]"

the extra ``[dev]`` arguments extract all packages for development

.. warning::
    pip version must be *22.0.0* or higher, otherwise try ``$ pip install -U pip`` before.

Pre-commit hook
---------------

We use `pre-commit`_ to reduce linting and formatting fix at Pull request time, some hooks are define in .pre-commit-config.yaml
To install `pre-commit`_, after you have install and activate your python environment, run::

    $ pre-commit install

to use pre-commit out of the git hooks context, run::

    $ pre-commit run <hook-id>

or::

    $ pre-commit run --all-files

Testing
=======

Test are writed using `pytest`_ and provide in the ``tests`` folder.
To run all the test, be sure to have the needed data or filter them by marks (describe below).

Also, we have some mark to help to categorized test and only run unit test or integration test.

- to mark unit tests, please mark them with ``@pytest.mark.integration``
- to mark integration tests, please mark them with ``@pytest.mark.unit``
- to mark tests that need an external file, please mark them with ``@pytest.mark.need_files``
- to mark tests that need s3 storage, please mark them with ``@pytest.mark.real_s3``

Our test use ``@pytest.fixture``, ``@pytest.mark.parametrize``, `hypothesis`_ decorators to make them configurable.

`pytest`_ use the ``conftest.py`` file to provide fixture for all test without importing them.
We also use the plugin ``pytest-lazy-fixture`` to retrieve fixture inside ``@pytest.mark.parametrize``.

External test data
------------------

Our test data are managed as ``@pytest.fixture``.

An environment variable ``TEST_DATA_FOLDER`` is used to retrieve them, or in ``/<project-folder>/data`` by default.

Currently, external test data concern:
- legacy product (S3, S2, S1, etc ...)

You can setup the an output folder to write data with ``TEST_OUTPUT_FOLDER``.
If is not set, a tempory folder is used.

Example
~~~~~~~

* set a TEST_DATA_FOLDER:

    - ``TEST_DATA_FOLDER=local_data/ pytest tests -m unit``
    - ``TEST_DATA_FOLDER=.data/ pytest tests``
    - ``TEST_DATA_FOLDER=/home/user/data/ pytest tests -m integration``

* set a TEST_OUTPUT_FOLDER:

    - ``TEST_OUTPUT_FOLDER=/tmp/ouput_test pytest tests -m unit``
    - ``TEST_OUTPUT_FOLDER=.output_dir/ pytest tests``
    - ``TEST_OUTPUT_FOLDER=data_out/ pytest tests -m integration``

* set both TEST_OUTPUT_FOLDER and TEST_DATA_FOLDER:

    - ``TEST_OUTPUT_FOLDER=/tmp/output_data TEST_DATA_FOLDER=local_data/ pytest tests -m unit``
    - ``TEST_OUTPUT_FOLDER=output_dir/ TEST_DATA_FOLDER=.data/ pytest tests``
    - ``TEST_OUTPUT_FOLDER=.data_out/ TEST_DATA_FOLDER=/home/user/data/ pytest tests -m integration``

* or using default values (data comes from ``data/`` and output folder a tempory folder ``/tmp/<something>``)

    - ``pytest tests -m unit``
    - ``pytest tests``
    - ``pytest tests -m integration``

Coverage
~~~~~~~~

The coverage is checked using `pytest-cov`_, a pytest plugin for `coverage`_.

Linting and formatting
======================

To ensure that our code follow the coding guide lines, we use:

* `flake8`_ as linter to be sure code convention are respected, with the ``.flake8`` configuration file
* `black`_ as formatter to rewrite some coding part and uniformized it, with configuration in the ``pyproject.toml`` (section ``[tool.black]``)
* `isort`_ as ``import`` formatter to reformat ``import`` order, with configuration in the ``pyproject.toml`` (section ``[tool.isort]``)

Typing
======

To check that our python code is well typed, we use `mypy`_, with the configuration provide in ``.mypy.ini`` file.

Documentation
=============

Documentation files are prodide inside the ``docs/source`` folder.
To build it, we use `sphinx`_ with many plugins:

* **jupyter-sphinx**: provide command to write jupyter sequence
* **sphinx-multiversion**: helper to create the documentation for multiple version of the same package
* **sphinxcontrib-apidoc**: generate API documentation automatically from the source code.

For the code documentation, our code follow the `numpydoc`_ style

Coverage
~~~~~~~~

To check documentation coverage, we use `docstr-coverage`_.
It provide a way to specify inherited documentation for classes: ``# docstr-coverage: inherited``


Security Check
==============

Different security check are made:

* static code analysis: checked with `bandit`_
* dependancies analysis: checked with `safety`_


Method / Function order
=======================

We try to follow the following scope order to have harmonized structure:

* Global attributes
* *__init__* (for classes)
* magic dunders methods (for classes)
* public property/methods/function
* protected property/methods/function
* private property/methods/function
* public staticmethod and classmethod (for classes)
* protected staticmethod and classmethod (for classes)
* private staticmethod and classmethod (for classes)

Each scope follow the alphabetic order

Git Commit Convention
=====================

For commits, we use the `Conventional commits specification`_

Type
----

Must be one of the following:

* **build**: Changes that affect the build system or external dependencies (example scopes: gulp, broccoli, npm)
* **ci**: Changes to our CI configuration files and scripts (example scopes: Travis, Circle, BrowserStack, SauceLabs)
* **docs**: Documentation only changes
* **feat**: A new feature
* **fix**: A bug fix
* **perf**: A code change that improves performance
* **refactor**: A code change that neither fixes a bug nor adds a feature
* **style**: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
* **test**: Adding missing tests or correcting existing tests


.. _pre-commit: https://pre-commit.com/
.. _pytest: https://docs.pytest.org/en/7.0.x/
.. _Conventional commits specification: https://www.conventionalcommits.org/en/v1.0.0/
.. _flake8: https://flake8.pycqa.org/en/latest/
.. _mypy: http://www.mypy-lang.org/
.. _bandit: https://bandit.readthedocs.io/en/latest/
.. _safety: https://github.com/pyupio/safety
.. _sphinx: https://www.sphinx-doc.org/en/master/
.. _numpydoc: https://numpydoc.readthedocs.io/en/latest/format.html
.. _coverage: https://coverage.readthedocs.io/en/latest/
.. _docstr-coverage: https://github.com/HunterMcGushion/docstr_coverage
.. _pytest-cov: https://github.com/pytest-dev/pytest-cov
.. _hypothesis: https://hypothesis.readthedocs.io/en/latest/
