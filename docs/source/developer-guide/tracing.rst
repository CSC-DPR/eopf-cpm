Tracing
=================================

We separate tracing into two:

- function based tracing, i.e. **profiling**
- system based continoous tracing

Profiling
------------------------------------------

For function profiling we provide the developpers two parametrisable
decorators,  which are meant to give parralel processing feedback
(through `dask`_) and single thread profiling (through pstats):

- :py:func:`~eopf.tracing.profiler.dask_profiler`
- :py:func:`~eopf.tracing.profiler.single_thread_profiler`

Both profilers are available in the :py:mod:`eopf.tracing module`.

    .. code-block:: python

        from eopf.tracing import dask_profiler, single_thread_profiler

dask_profiler()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Below we present a realistic example of a developper's defined function.
As you can see the function has been decorated with the :py:func:`~eopf.tracing.profiler.dask_profiler`.
Thus after the execution of the actual function, the developper will
get a `dask`_ generated report in html format. In case the developper
is runnig the code in an interactive Ipython console, then the html
report will be automatically loaded.


    .. code-block:: python

        @dask_profiler(report_name="demo/default_cluster.html")
        def s3_to_nc():
            from eopf.product.store.netcdf import EONetCDFStore
            from eopf.product.store.safe import EOSafeStore
            from eopf.product.store.conveniences import convert

            safe_store = EOSafeStore("data/S3A_OL_1_EFR.SEN3")
            nc_store = EONetCDFStore("data/olci.nc")
            convert(safe_store, nc_store)

By default the :py:func:`~eopf.tracing.profiler.dask_profiler` will run on a predefined dask cluster.
However the developper can request a specific cluster configuration
through the decorator's parameters, i.e. : n_workers and threads_per_worker;
see example below.

    .. code-block:: python

        @dask_profiler(
            n_workers=4,
            threads_per_worker=2,
            report_name="user_defined_cluster.html")
        def s3_to_nc():
            ...

It is possible to disable the displaying of the html report, even if
the developper is working in an Ipython interactive console, by
seeting the parameter *display_report* to False.

The *report_name* parameter can be set such that the report can be
written at any particular path.

The developper can also disable the decorator by setting the *enable*
parameter to False. This allows to leave the decorator syntax in place
without modifying the functionality of the decorated function.


single_thread_profiler()
~~~~~~~~~~~~~~~~~~~~~~~

With single_thread_profiler the developper gets *cProfile*
statistics (pstats.Stats) of the decorated function. Further more,
these statistics can be written at a specific path through the
*report_name* parameter.

    .. code-block:: python

        @single_thread_profiler(report_name="report.dump")
        def s3_to_nc():
            from eopf.product.store.netcdf import EONetCDFStore
            from eopf.product.store.safe import EOSafeStore
            from eopf.product.store.conveniences import convert

            safe_store = EOSafeStore("data/S3A_OL_1_EFR.SEN3")
            nc_store = EONetCDFStore("data/olci.nc")
            convert(safe_store, nc_store)

        stats = s3_to_nc()


One can further manipulate the stats. For example, the code below
strips the directory path from python functions, such that one can
observe the functions more easily. Also, the statistics are ordered
according to the total time spent in a function.

    .. code-block:: python

        stats.strip_dirs().sort_stats('tottime').print_stats()


It is possible to run both profilers at the same time, as depicted
below. The single_thread_profiler must be the first one.

    .. code-block:: python

        @single_thread_profiler(report_name="stats_report.dump")
        @dask_profiler(report_name="dask_report.html")
        def s3_to_nc():
            ...

        stats = s3_to_nc()


System based continoous tracing
------------------------------------------

For continous tracking we refer the developpers and users to
*dask dashboard*, usually located at <http://127.0.0.1:8787/status>.
If the port or address is different, just change the url with your
specific dask running port and address.


We also recommended using `Prometheus`_ for monitoring dask performace
over time. We provide a default `Prometheus`_
configuration file, *prometheus-cpm.yml* , with the :py:mod:`eopf.tracing` module.


.. _dask: https://www.dask.org/
.. _Prometheus: https://prometheus.io/
