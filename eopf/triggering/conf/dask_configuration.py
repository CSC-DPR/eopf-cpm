import importlib
from typing import Any, Optional

from dask.distributed import Client
from importlib_metadata import PackageNotFoundError


class DaskContext:
    """Representation of a dask context used to run
    dask with the correct configuration.

    Parameters
    ----------
    cluster_type: type of cluster use
        can be one of:

            * **None**: disable distributed execution
            * **local**: configure a :py:class:`~dask.distributed.LocalCluster`
            * **ssh**: configure a :py:func:`~dask.distributed.SSHCluster`
            * **kubernetes**: configure a :py:class:`~dask_kubernetes.KubeCluster`
            * **pbs**: configure a :py:class:`~dask_jobqueue.PBSCluster`
            * **sge**: configure a :py:class:`~dask_jobqueue.SGECluster`
            * **lsf**: configure a :py:class:`~dask_jobqueue.LSFCluster`
            * **slurm**: configure a :py:class:`~dask_jobqueue.SLURMCluster`
            * **slurm**: configure a :py:class:`~dask_jobqueue.SLURMCluster`
            * **yarn**: configure a :py:class:`~dask_yarn.YarnCluster`
            * **gateway**: configure a :py:class:`~dask_gateway.GatewayCluster`
            * **str**: to pass simple cluster addr
            * **custom**: to use Custom cluster class specified by following element in **cluster_config** element.

                - **module**: python path to module containing custom cluster
                - **cluster**: cluster class name
    addr: str
        only for **str** cluster_type, specified cluster adresse to join.
    cluster_config: dict
        key value pairs of parameters to give to the cluster
    client_config: dict
        element to configure :py:class:`~dask.distributed.Client`
    """

    def __init__(
        self,
        cluster_type: Optional[str] = None,
        addr: Optional[Any] = None,
        cluster_config: dict[str, Any] = {},
        client_config: dict[str, Any] = {},
    ) -> None:
        cluster: Optional[Any] = None
        if cluster_type is None:
            pass
        elif cluster_type == "local":
            from dask.distributed import LocalCluster

            cluster = LocalCluster(**cluster_config)
        elif cluster_type == "ssh":
            from dask.distributed import SSHCluster

            cluster = SSHCluster(**cluster_config)
        elif cluster_type == "kubernetes":
            try:
                from dask_kubernetes import KubeCluster

                cluster = KubeCluster(**cluster_config)
            except ModuleNotFoundError:
                raise PackageNotFoundError("Package dask_kubernetes should be installed.")
        elif cluster_type == "pbs":
            try:
                from dask_jobqueue import PBSCluster

                cluster = PBSCluster(**cluster_config)
            except ModuleNotFoundError:
                raise PackageNotFoundError("Package dask_jobqueue should be installed.")
        elif cluster_type == "sge":
            try:
                from dask_jobqueue import SGECluster

                cluster = SGECluster(**cluster_config)
            except ModuleNotFoundError:
                raise PackageNotFoundError("Package dask_jobqueue should be installed.")
        elif cluster_type == "lsf":
            try:
                from dask_jobqueue import LSFCluster

                cluster = LSFCluster(**cluster_config)
            except ModuleNotFoundError:
                raise PackageNotFoundError("Package dask_jobqueue should be installed.")
        elif cluster_type == "slurm":
            try:
                from dask_jobqueue import SLURMCluster

                cluster = SLURMCluster(**cluster_config)
            except ModuleNotFoundError:
                raise PackageNotFoundError("Package dask_jobqueue should be installed.")
        elif cluster_type == "yarn":
            try:
                from dask_yarn import YarnCluster

                cluster = YarnCluster(**cluster_config)
            except ModuleNotFoundError:
                raise PackageNotFoundError("Package dask_yarn should be installed.")
        elif cluster_type == "gateway":
            try:
                import dask
                from dask_gateway import GatewayCluster
                from dask_gateway.auth import get_auth

                # one of ("kerberos", "jupŷterhub", "basic") or a python pass to the auth class
                auth_kwargs = cluster_config.pop("auth", {})
                workers = cluster_config.pop("workers", 1)
                auth_type = auth_kwargs.pop("auth")
                dask.config.set({"gateway.auth.kwargs": auth_kwargs})
                auth = get_auth(auth_type)

                cluster = GatewayCluster(**cluster_config, auth=auth)
                if isinstance(workers, int):
                    cluster.scale(workers)
                elif isinstance(workers, dict):
                    cluster.adapt(**workers)
            except ModuleNotFoundError:
                raise PackageNotFoundError("Package dask_yarn should be installed.")
        elif cluster_type == "str":
            cluster = addr
        elif cluster_type == "custom":
            try:
                module_name = cluster_config.pop("module")
                cluster_class_name = cluster_config.pop("cluster")
                cluster = getattr(importlib.import_module(module_name), cluster_class_name)(**cluster_config)
            except ModuleNotFoundError:
                raise ModuleNotFoundError(f"Module {module_name} not found, corresponding package should be installed")
        else:
            raise Exception("Invalid dask context configuration")
        self._cluster = cluster
        self._client: Optional[Client] = None
        self._client_config = client_config

    def __enter__(self) -> "DaskContext":
        if self._cluster is not None:
            self._cluster.__enter__()
            self._client = Client(self._cluster, **self._client_config)
            self._client.__enter__()
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        if self._cluster is not None:
            self._cluster.__exit__(*args, **kwargs)

        if self._client is not None:
            self._client.__exit__(*args, **kwargs)
            self._client = None
