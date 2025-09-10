"""Test the metrics library functions."""
import numpy as np
import pytest

from sheerwater_benchmarking.utils import start_remote
from sheerwater_benchmarking.metrics_library import get_bins, metric_factory

start_remote(remote_config='large_cluster')


def test_get_bins():
    """Test the get_bins function."""
    assert all(get_bins('mae') == np.array([]))
    assert all(get_bins('pod-5') == np.array([-np.inf, 5, np.inf]))
    assert all(get_bins('pod-5-10') == np.array([-np.inf, 5, 10, np.inf]))
    with pytest.raises(ValueError):
        get_bins('pod-abc')


def test_metric_factory():
    """Test the metric factory function."""
    cache_kwargs = {
        'start_time': '2016-01-01',
        'end_time': '2022-12-31',
        'variable': 'precip',
        'lead': 'week3',
        'forecast': 'ecmwf_ifs_er_debiased',
        'truth': 'era5',
        'time_grouping': None,
        'spatial': False,
        'grid': 'global1_5',
        'mask': 'lsm',
        'region': 'kenya',
    }

    met = metric_factory('heidke-1-5-10-20', **cache_kwargs)
    test = met.compute()
    assert test is not None


if __name__ == "__main__":
    start_remote(remote_config='large_cluster')
    test_metric_factory()
