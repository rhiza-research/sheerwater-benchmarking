"""Test the caching functions in the cacheable decorator."""
import pytest
import numpy as np
import pandas as pd
import xarray as xr
from sheerwater_benchmarking.utils import cacheable, get_dates


@cacheable(data_type='array',
           timeseries='time',
           cache_args=['name', 'species'])
def simple_timeseries(start_time, end_time, name, species='coraciidae'):
    """Generate a simple timeseries dataset for testing."""
    times = get_dates(start_time, end_time, stride='day', return_string=False)
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
    ds1 = simple_timeseries(start_time, end_time, name)

    # Run again with null time
    ds2 = simple_timeseries(None, None, name)

    # Ensure the two datasets are equal
    assert ds1.equals(ds2)

    # Test without caching
    name = 'indian roller'
    with pytest.raises(ValueError):
        ds1 = simple_timeseries(None, None, name)


@cacheable(data_type='tabular',
           timeseries='time',
           backend='arrow',
           cache_args=['species'])
def tabular_timeseries(start_time, end_time, species='coraciidae'):
    """Generate a simple tabular timeseries dataset for testing."""
    times = get_dates(start_time, end_time, stride='day', return_string=False)
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = pd.DataFrame({'obs': obs, 'time': times})
    return ds


def test_tabular_timeseries():
    start_time = '2020-01-01'
    end_time = '2020-01-10'

    ds1 = tabular_timeseries(start_time, end_time, recompute=True, force_overwrite=True)

    end_time = '2020-01-15'
    # Without validate_cache_timeseries, this should return only the original 10 days (and the same values, not
    # new random numbers).
    ds2 = tabular_timeseries(start_time, end_time, validate_cache_timeseries=False)

    # Don't use .equals(), because it's OK if times are stored as datetime64[ns] and restored as datetime64[us]
    assert (ds1.columns == ds2.columns).all()
    assert (ds1 == ds2).all().all()

    end_time = '2020-01-07'
    ds3 =  tabular_timeseries(start_time, end_time, validate_cache_timeseries=False)
    assert len(ds3) < len(ds1)


if __name__ == "__main__":
    test_null_time_caching()
    test_tabular_timeseries()
