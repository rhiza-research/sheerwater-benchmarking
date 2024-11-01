"""Utilities for running functions on a remote dask cluster."""
from dask.distributed import Client, get_client, LocalCluster
import coiled
import logging
from functools import wraps
import os
import pwd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

config_options = {
    'large_scheduler': {
        'scheduler_cpu': 16,
        'scheduler_memory': "64GiB"
    },
    'large_cluster': {
        'n_workers': 10
    },
    'xlarge_cluster': {
        'n_workers': 15
    },
    'xxlarge_cluster': {
        'n_workers': 25
    },
    'large_node': {
        'worker_vm_types': ['c2-standard-16', 'c3-standard-22']
    },
    'xlarge_node': {
        'worker_vm_types': ['c2-standard-32', 'c3-standard-44']
    },
}

def start_remote(remote_name=None, remote_config=None):
    default_name = 'sheerwater_' + pwd.getpwuid(os.getuid())[0]

    coiled_default_options = {
        'name': default_name,
        'n_workers': [3, 8],
        'idle_timeout': "120 minutes",
        'scheduler_cpu': 8,
        'scheduler_memory': "32GiB",
        'worker_vm_types': ['c2-standard-8', 'c3-standard-8'],
        'spot_policy': 'spot_with_fallback',
    }

    if  remote_name and isinstance(remote_name, str):
        coiled_default_options['name'] = remote_name

    if remote_config and isinstance(remote_config, dict):
        # setup coiled cluster with remote config
        logger.info("Attaching to coiled cluster with custom configuration")
        coiled_default_options.update(remote_config)
    elif remote_config and (isinstance(remote_config, str) or
                                        isinstance(remote_config, list)):
        logger.info("Attaching to coiled cluster with preset configuration")
        if not isinstance(remote_config, list):
            remote_config = [remote_config]

        for conf in remote_config:
            if conf in config_options:
                coiled_default_options.update(config_options[conf])
            else:
                print(f"Unknown preset remote config option {conf}. Skipping.")
    else:
        # Just setup a coiled cluster
        logger.info("Attaching to coiled cluster with default configuration")

    cluster = coiled.Cluster(**coiled_default_options)
    cluster.get_client()


def dask_remote(func):
    """Decorator to run a function on a remote dask cluster."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # See if there are extra function args to run this remotely
        if 'remote' in kwargs and kwargs['remote']:

            remote_name = None
            if 'remote_name' in kwargs:
                remote_name = kwargs['remote_name']

            remote_config = None
            if 'remote_config' in kwargs:
                remote_config = kwargs['remote_config']

            start_remote(remote_name, remote_config)

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

        if 'remote_name' in kwargs:
            del kwargs['remote_name']

        return func(*args, **kwargs)
    return wrapper
