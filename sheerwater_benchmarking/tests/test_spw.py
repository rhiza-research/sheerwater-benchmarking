"""Tests for the suitable planting window model."""
import numpy as np
from sheerwater_benchmarking.metrics import get_datasource_fn
from sheerwater_benchmarking.utils import start_remote
from sheerwater_benchmarking.forecasts.ecmwf_er import ifs_extended_range_spw
from sheerwater_benchmarking.baselines.climatology import climatology_spw


def test_spw():
    """Test that the SPW can successfully be called as a variable for all forecasters."""
    start_time = "2016-01-01"
    end_time = "2022-12-31"
    datasets = []
    for forecast in ["ecmwf_ifs_er", "ecmwf_ifs_er_debiased", "climatology_2015"]:
        fn = get_datasource_fn(forecast)
        datasets.append(fn(start_time, end_time,
                           'rainy_onset',
                           lead='day11',
                           prob_type='deterministic',
                           grid='global1_5', mask='lsm', region='kenya').assign_attrs(forecast=forecast))

    for ds in datasets:
        assert ds.time.size == 14
        assert np.issubdtype(ds.rainy_onset.dtype, np.datetime64)

    # Test calling SPW and getting a raw probability time series for ECMWF IFS ER (debiased and non-debiased)
    datasets = []
    for debiased in [False, True]:
        datasets.append(ifs_extended_range_spw(start_time, end_time, 'day11',  debiased=debiased,
                                               prob_type='probabilistic', prob_threshold=None,
                                               onset_group=None, aggregate_group=None,
                                               grid='global1_5', mask='lsm', region='kenya'))

    for ds in datasets:
        assert ds.time.size == 730
        assert np.issubdtype(ds.rainy_onset.dtype, np.datetime64)

    # Test calling SPW and getting a raw probability time series for climatology
    ds = climatology_spw(first_year=2004, last_year=2015,
                         prob_type='probabilistic', prob_threshold=None,
                         onset_group=None, aggregate_group=None,
                         grid='global1_5', mask='lsm', region='kenya')
    assert ds.time.size == 730
    assert np.issubdtype(ds.rainy_onset.dtype, np.datetime64)


if __name__ == "__main__":
    start_remote()
    test_spw()
