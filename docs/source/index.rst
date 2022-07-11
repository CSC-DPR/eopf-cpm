.. include:: ../../README.rst

The **Core Python Modules ( CPM )** framework comes with the main following modules and packages:

:py:mod:`eopf.product`:
   provides a homogenous access interface to the legacy Copernicus products and
   convenience functions for reading and writing of new structured :py:class:`~eopf.product.core.eo_product.EOProduct`.
:py:mod:`eopf.computing`:
   provides to the re-engineered processor developers a homogenous API allowing
   the implementation of the parallelism features whatever the execution context.
:py:mod:`eopf.triggering`:
   allows defining a re-engineered processor’s workflow as well as its triggering and allows passing
   the configurations needed by the processing units forming the workflow.
:py:mod:`eopf.logging`:
   provides a simple and homogeneous interface to the logging system.
:py:mod:`eopf.tracing`:
   provides a simple and homogeneous interface to the tracing system.
:py:mod:`eopf.numerical`:
   provides a set of helper functions for the image and signal processing.
:py:mod:`eopf.auxdata`:
   provides readers functions for the common auxiliary data files.
:py:mod:`eopf.qualitycontrol`:
   Provides a generic :py:class:`~eopf.qualitycontrol.eo_qc_processor.EOQCProcessor` that allows applying a series of
   quality control checks on the re-engineered processor’s output products.

The :ref:`user-guide` aims to get you using eopf cpm productively as quickly as possible.
It is designed as an entry point for new users, and it provided an introduction to eopf cpm main concepts.

License
-------

EOPF CPM is available under the open source `Apache License`__.

__ https://www.apache.org/licenses/LICENSE-2.0.html

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: For users

   Installation <installation>
   User Guide <user-guide/index>
   Developer Guide <developer-guide/index>
   API Reference <api/eopf>

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: For EOPF CPM contributors

   Contributing Guide <contributing>
   GitHub repository <https://github.com/CSC-DPR/eopf-cpm>
