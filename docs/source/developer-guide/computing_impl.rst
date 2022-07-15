How to develop new processor based on CPM
=========================================

EOPF-CPM provide abstract classes to help users to write algorithm with EO Object
(:py:class:`~eopf.product.core.eo_product.EOProduct`, :py:class:`~eopf.product.core.eo_group.EOGroup`, :py:class:`~eopf.product.core.eo_variable.EOVariable`).


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

that are direct mapping of :py:func:`dask.array.map_blocks` (resp. :py:func:`dask.array.map_overlap`) that let you define only
the low level function passed into the corresponding dask function.


Examples
--------

Examples of processors are available in those projects:

* `S3 OLCI L2 Processor`_
* `S2 Processor`_

.. _S3 OLCI L2 Processor: https://gitlab.csc-eopf.csgroup.space/cpm/s3-olci-l2
.. _S2 Processor: https://gitlab.csc-eopf.csgroup.space/eopf-s2-proc/eopf-s2-proc
