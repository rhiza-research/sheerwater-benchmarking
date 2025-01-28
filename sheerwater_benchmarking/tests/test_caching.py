"""Test the caching functions in the cacheable decorator."""
import pytest
import numpy as np
import pandas as pd
import xarray as xr
from sheerwater_benchmarking.utils import cacheable, get_dates


@cacheable(data_type='array',
           timeseries='time',
           cache_args=['name', 'species', 'stride'])
def simple_timeseries(start_time, end_time, name, species='coraciidae', stride='day'):
    """Generate a simple timeseries dataset for testing."""
    times = get_dates(start_time, end_time, stride=stride, return_string=False)
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
    ds.attrs['name'] = name
    ds.attrs['species'] = species
    return ds


def test_null_time_caching():
    """Cache a simple timeseries dataset with a null time."""
    start_time = '2020-01-01'
    end_time = '2020-01-10'
    name = 'lilac-breasted roller'

    # Run once to ensure simple timeseries is cached
    ds1 = simple_timeseries(start_time, end_time, name, recompute=True, force_overwrite=True)

    # Run again with null time
    ds2 = simple_timeseries(None, None, name)

    # Ensure the two datasets are equal
    assert ds1.equals(ds2)

    # Test without caching
    name = 'indian roller'
    with pytest.raises(ValueError):
        ds1 = simple_timeseries(None, None, name)


def test_validate_timeseries():
    """
    Test triggering recompute with validate_cache_timeseries=True.

    If there's no data within a year of the requested start or end times, and validate_cache_timeseries=True, we should
    recompute. This is tested with array data because the check isn't supported for tabular data yet.
    """
    name = "racket-tailed roller"
    start_time = "2018-01-01"
    end_time = "2020-01-01"
    ds1 = simple_timeseries(
        start_time, end_time, name, stride="month", recompute=True, force_overwrite=True
    )

    # 6 months earlier -> should not recompute
    start_time = "2017-06-01"
    ds2 = simple_timeseries(
        start_time, end_time, name, stride="month", validate_cache_timeseries=True
    )
    assert ds1.equals(ds2)

    # 18 months earlier, but validate_cache_timeseries=False -> should not recompute
    start_time = "2017-06-01"
    ds3 = simple_timeseries(
        start_time, end_time, name, stride="month", validate_cache_timeseries=False
    )
    assert ds1.equals(ds3)

    # 18 months earlier -> should recompute
    start_time = "2016-06-01"
    # force_overwrite to avoid reading from stdin during test
    ds4 = simple_timeseries(
        start_time,
        end_time,
        name,
        stride="month",
        validate_cache_timeseries=True,
        force_overwrite=True,
    )
    assert len(ds1.time) < len(ds4.time)



@cacheable(data_type='tabular',
           timeseries='time',
           backend='parquet',
           cache_args=['species'])
def tabular_timeseries(start_time, end_time, species='coraciidae'):
    """Generate a simple tabular timeseries dataset for testing."""
    times = get_dates(start_time, end_time, stride='day', return_string=False)
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = pd.DataFrame({'obs': obs, 'time': times})
    return ds


def test_tabular_timeseries():
    """
    Test timeseries caching with data_type='tabular'.

    Requesting more days of data should return the same thing, because we can't yet
    validate_cache_timeseries. Requesting fewer should return less data, though.

    We can tell that a function is cached by having it return random data, and checking if the
    randomness is consistent between calls.
    """

    start_time = '2020-01-01'
    end_time = '2020-01-10'

    ds1 = tabular_timeseries(start_time, end_time, recompute=True, force_overwrite=True, backend="parquet")

    end_time = '2020-01-15'
    # Without validate_cache_timeseries, this should return only the original 10 days (and the same values, not
    # new random numbers).
    ds2 = tabular_timeseries(
        start_time, end_time, backend="parquet", validate_cache_timeseries=False
    )

    # Don't use .equals(), because it's OK if times are stored as datetime64[ns] and restored as datetime64[us]
    assert (ds1.columns == ds2.columns).all()
    assert (ds1 == ds2.compute()).all().all()

    end_time = '2020-01-07'
    ds3 = tabular_timeseries(
        start_time, end_time, backend="parquet", validate_cache_timeseries=False
    )
    assert len(ds3) < len(ds1)


if __name__ == "__main__":
    test_null_time_caching()
    test_validate_timeseries()
    test_tabular_timeseries()
