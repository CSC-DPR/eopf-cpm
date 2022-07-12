"""The objective of the eopf.tracing module is to provide a simple and homogeneous interface to the tracing system.
We further divide tracing in two: function-based tracing and continuous tracing.

For function-based tracing we provide the developers two parametrizable decorators,
which are ment to give parallel processing feedback (through dask) and single thread profiling (through pstats).
Thus, the developer can easily trace the performance of its code in distributed and non-distributed environments.
The dask_profiler generates a dask provided html report which can be saved to a configurable location.
Also, it provides the developer the ability to run on al already defined dask cluster or on a configurable cluster.
The single_thread_profiler generates and returns a pstats object with multiple function-based statistics.
The statistics can be easily manipulated and filtered, for better insight.
The pstats object can also be saved to a configurable location,

Since dask is used as base for all eopf-cpf computations,
we refer the developers and users to the standard dask dashboard as the standard continous tracing method.
Nevertheless, we also recommended using Prometheus for monitoring dask performace over time.
We provide a default Prometheus configuration file, i.e. prometheus-cpm.yml , within then eopf.tracing folder,
for rapid configuration.
"""

from .profiler import (
    dask_profiler,
    distributed_progress,
    local_progress,
    single_thread_profiler,
)

__all__ = [
    "dask_profiler",
    "single_thread_profiler",
    "local_progress",
    "distributed_progress",
]
