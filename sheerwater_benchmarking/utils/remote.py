"""Utilities for running functions on a remote dask cluster."""
from dask.distributed import Client, get_client, LocalCluster
import coiled
import logging
from functools import wraps
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def dask_remote(func):
    """Decorator to run a function on a remote dask cluster."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # See if there are extra function args to run this remotely
        if 'remote' in kwargs and kwargs['remote']:

            coiled_default_options = {
                'name': 'sheerwater_shared',
                'n_workers': [3, 8],
                'idle_timeout': "120 minutes",
                # 'scheduler_cpu': 8,
                # 'scheduler_memory': "32GiB",
                # 'worker_vm_types': ['c2-standard-8', 'c3-standard-8'],
                'spot_policy': 'spot_with_fallback',
            }

            if 'remote_config' in kwargs:
                # setup coiled cluster with remote config
                logger.info("Attaching to coiled cluster with custom configuration")
                coiled_default_options.update(kwargs['remote_config'])
                cluster = coiled.Cluster(**coiled_default_options)
                cluster.get_client()
            else:
                # Just setup a coiled cluster
                logger.info("Attaching to coiled cluster with default configuration")
                cluster = coiled.Cluster(**coiled_default_options)
                cluster.get_client()
        else:
            # Setup a local cluster
            try:
                get_client()
            except ValueError:
                logger.info("Starting local dask cluster...")
                cluster = LocalCluster(n_workers=2, threads_per_worker=2)
                Client(cluster)

        # call the function and return the result
        if 'remote' in kwargs:
            del kwargs['remote']

        if 'remote_config' in kwargs:
            del kwargs['remote_config']

        return func(*args, **kwargs)
    return wrapper
