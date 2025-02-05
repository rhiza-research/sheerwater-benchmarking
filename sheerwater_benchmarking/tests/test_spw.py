"""Tests for the suitable planting window model."""
import numpy as np
from sheerwater_benchmarking.metrics import get_datasource_fn
from sheerwater_benchmarking.utils import start_remote


def test_spw():
    """Test that the SPW can successfully be called as a variable for all forecasters."""
    start_time = "2016-01-01"
    end_time = "2022-12-31"
    datasets = []
    for forecast in ["ecmwf_ifs_er", "ecmwf_ifs_er_debiased", "salient", "climatology_2015"]:
        fn = get_datasource_fn(forecast)
        datasets.append(fn(start_time, end_time,
                           'rainy_onset',
                           lead='day11',
                           prob_type='deterministic',
                           grid='global1_5', mask='lsm', region='kenya').assign_attrs(forecast=forecast))

    for ds in datasets:
        assert ds.time.size == 14
        assert np.issubdtype(ds.rainy_onset.dtype, np.datetime64)


if __name__ == "__main__":
    start_remote()
    test_spw()
