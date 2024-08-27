"""Utilities for running functions on a remote dask cluster."""
from dask.distributed import Client, get_client, LocalCluster
import coiled
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def dask_remote(func):
    """Decorator to run a function on a remote dask cluster."""
    def wrapper(*args, **kwargs):
        # See if there are extra function args to run this remotely
        if 'remote' in kwargs and kwargs['remote']:
            if 'remote_config' in kwargs:
                # setup coiled cluster with remote config
                logger.info("Attaching to coiled cluster with custom configuration")
                cluster = coiled.Cluster(**kwargs['remote_config'])
                client = cluster.get_client()

                del kwargs['remote_config']
            else:
                # Just setup a coiled cluster
                logger.info("Attaching to coiled cluster with default configuration")
                cluster = coiled.Cluster(name='sheerwater-shared',
                                         n_workers=[3, 10], idle_timeout="45 minutes")
                client = cluster.get_client()

            del kwargs['remote']
        else:
            # Setup a local cluster
            try:
                client = get_client()
            except ValueError:
                logger.info("Starting local dask cluster...")
                cluster = LocalCluster(n_workers=2, threads_per_worker=2)
                client = Client(cluster)

        # call the function and return the result
        return func(*args, **kwargs)
    return wrapper
