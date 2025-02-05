"""Tests for the suitable planting window model."""
from sheerwater_benchmarking.forecasts.ecmwf_er import ifs_extended_range_spw


def test_spw():
    """Test that the SPW can successfully be called."""
    start_time = "2016-01-01"
    end_time = "2022-12-31"
    ds = ifs_extended_range_spw(
        start_time, end_time,
        prob_type='probabilistic',
        lead='day11',
        prob_threshold=None,
        groupby=None,
        grid='global1_5',
        mask='lsm',
        region='kenya',
    )
    assert ds

    ds = ifs_extended_range_spw(
        start_time, end_time,
        prob_type='probabilistic',
        lead='day11',
        prob_threshold=0.6,
        groupby=[['ea_rainy_season', 'year']],
        grid='global1_5',
        mask='lsm',
        region='kenya',
    )
    assert ds
