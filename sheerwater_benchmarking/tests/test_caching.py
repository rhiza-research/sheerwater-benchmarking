"""Test the caching functions in the cacheable decorator."""
import pytest
import numpy as np
import xarray as xr
from sheerwater_benchmarking.utils import cacheable, get_dates
from sheerwater_benchmarking.utils.caching import cacheable


@cacheable(data_type='array',
           timeseries='time',
           cache_args=['name', 'species'],
           cache=False)
def simple_timeseries2(start_time, end_time, name, species='coraciidae'):
    """Generate a simple timeseries dataset for testing."""
    return None


def test_cacheable_arg_override():
    """Test the cacheable decorator with overridden arguments."""
    start_time = '2020-01-01'
    end_time = '2020-01-10'
    name = 'lilac-breasted roller'

    # Run once to ensure simple timeseries is cached
    ds1 = simple_timeseries2(start_time, end_time, name, cache=True,
                             recompute=True, dont_recompute='simple_timeseries2')

    # Run again with overridden arguments
    ds2 = simple_timeseries2(start_time, end_time, name, species='corvidae')

    # Test without caching
    name = 'indian roller'
    with pytest.raises(ValueError):
        ds1 = simple_timeseries2(start_time, end_time, name)


if __name__ == "__main__":
    test_cacheable_arg_override()
    print("All tests passed")


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
