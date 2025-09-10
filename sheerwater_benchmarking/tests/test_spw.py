"""Tests for the suitable planting window model."""
import numpy as np
from sheerwater_benchmarking.metrics import get_datasource_fn
from sheerwater_benchmarking.utils import start_remote
from sheerwater_benchmarking.forecasts.ecmwf_er import ecmwf_ifs_spw
from sheerwater_benchmarking.forecasts.climatology import climatology_spw


def test_spw():
    """Test that the SPW can successfully be called as a variable for all forecasters."""
    start_time = "2016-01-01"
    end_time = "2022-12-31"
    prob_types = ['deterministic', 'probabilistic']
    datasets = []
    for forecast in ["ecmwf_ifs_er", "ecmwf_ifs_er_debiased", "climatology_2015"]:
        for prob_type in prob_types:
            fn = get_datasource_fn(forecast)
            datasets.append(fn(start_time, end_time,
                               'rainy_onset',
                               lead='day11',
                               prob_type=prob_type,
                               grid='global1_5', mask='lsm', region='kenya'))

    for ds in datasets:
        assert ds.time.size == 14
        assert np.issubdtype(ds.rainy_onset.dtype, np.datetime64)

    for data in ["era5", "chirps", "ghcn", "imerg", "tahmo"]:
        fn = get_datasource_fn(data)
        ds = fn(start_time, end_time,
                'rainy_onset', agg_days=None,
                grid='global1_5', mask='lsm', region='kenya')
        if data != 'tahmo':
            assert ds.time.size == 14
        assert np.issubdtype(ds.rainy_onset.dtype, np.datetime64)

    # Test calling SPW and getting a raw probability time series for ECMWF IFS ER
    ds = ecmwf_ifs_spw(start_time, end_time, 'day11',
                       prob_type='probabilistic', prob_threshold=None,
                       onset_group=None, aggregate_group=None,
                       grid='global1_5', mask='lsm', region='kenya')
    assert ds.time.size == 730
    assert np.issubdtype(ds.rainy_onset.dtype, np.datetime64)

    # Test calling SPW and getting a raw probability time series for climatology
    ds = climatology_spw(first_year=2004, last_year=2015,
                         prob_type='probabilistic', prob_threshold=None,
                         onset_group=None, aggregate_group=None,
                         grid='global1_5', mask='lsm', region='kenya')
    assert ds.time.size == 2557
    assert np.issubdtype(ds.rainy_onset.dtype, np.datetime64)


if __name__ == "__main__":
    start_remote()
    test_spw()
