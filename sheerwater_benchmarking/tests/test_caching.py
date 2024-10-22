"""Test the caching functions in the cacheable decorator."""
import pytest
import numpy as np
import xarray as xr
from sheerwater_benchmarking.utils import cacheable, get_dates


@cacheable(data_type='array',
           timeseries='time',
           cache_args=['name', 'species'])
def simple_timeseries(start_time, end_time, name, species='coraciidae'):
    obs = np.random.randint(0, 10, size=(10,))
    times = get_dates(start_time, end_time, stride='day', return_string=False)
    ds = xr.Dataset({'obs': ('time', obs)}, coords={'time': times})
    ds.attrs['name'] = name
    ds.attrs['species'] = species
    return ds


def test_null_time_caching():
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
