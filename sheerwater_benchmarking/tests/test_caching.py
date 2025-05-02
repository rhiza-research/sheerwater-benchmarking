"""Test the caching functions in the cacheable decorator."""
import os
import random
import string
import datetime
import pytest
import fsspec
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
    """Test triggering recompute with validate_cache_timeseries=True.

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
           validate_cache_timeseries=False,
           cache_args=['species'])
def tabular_timeseries(start_time, end_time, species='coraciidae'):  # noqa: ARG001
    """Generate a simple tabular timeseries dataset for testing.

    The 'species' argument is unused, but still matters because it's a cache key
    """
    times = get_dates(start_time, end_time, stride='day', return_string=False)
    obs = np.random.randint(0, 10, size=(len(times),))
    ds = pd.DataFrame({'obs': obs, 'time': times})
    return ds


def test_tabular_timeseries():
    """Test timeseries caching with data_type='tabular'.

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
    ds2 = tabular_timeseries(start_time, end_time)

    assert ds1.compute().equals(ds2.compute())

    end_time = '2020-01-07'
    ds3 = tabular_timeseries(start_time, end_time)
    assert len(ds3) < len(ds1)


def test_cache_disable_if():
    """Test cache_disable_if argument."""
    @cacheable(data_type='basic',
               cache_args=['agg_days'],
               cache_disable_if={'agg_days': 7})
    def cached_func(agg_days=7):  # noqa: ARG001
        return np.random.randint(1000)

    @cacheable(data_type='basic',
               cache_args=['agg_days'],
               cache_disable_if={'agg_days': [1, 7, 14]})
    def cached_func2(agg_days=7):  # noqa: ARG001
        return np.random.randint(1000)

    @cacheable(data_type='basic',
               cache_args=['agg_days', 'grid'],
               cache_disable_if=[{'agg_days': [1, 7, 14],
                                  'grid': 'global1_5'},
                                 {'agg_days': 8}])
    def cached_func3(agg_days=7, grid='global0_25'):  # noqa: ARG001
        return np.random.randint(1000)

    # Instantiate the cache
    ds = cached_func(agg_days=1)
    #  Cache should be enabled - these should be equal
    dsp = cached_func(agg_days=1)
    assert ds == dsp

    # Instantiate the cache
    ds = cached_func(agg_days=7)
    #  Cache should be disabled - these should be different random numbers
    dsp = cached_func(agg_days=7)
    assert ds != dsp

    # Should be disabled
    ds = cached_func2(agg_days=14)
    dsp = cached_func2(agg_days=14)
    assert ds != dsp

    # Should be disabled
    ds = cached_func3(agg_days=14, grid='global1_5')
    dsp = cached_func3(agg_days=14, grid='global1_5')
    assert ds != dsp

    # Should be enabled
    ds = cached_func3(agg_days=14, grid='global0_25')
    dsp = cached_func3(agg_days=14, grid='global0_25')
    assert ds == dsp

    # Should be disabled
    ds = cached_func3(agg_days=8)
    dsp = cached_func3(agg_days=8)
    assert ds != dsp

    # Should be enabled
    ds = cached_func3(agg_days=14)
    dsp = cached_func3(agg_days=14)
    assert ds == dsp


def test_cache_arg_scope():
    """Test that argument scope is not improperly global/inherited between calls."""
    @cacheable(data_type='basic',
               cache_args=['agg_days'],
               cache_disable_if={'agg_days': 7})
    def cached_func(agg_days=7):  # noqa: ARG001
        return np.random.randint(1000)

    # Instantiate the cache
    ds = cached_func(agg_days=1)
    #  Cache should be enabled - these should be equal
    dsp = cached_func(agg_days=1)
    assert ds == dsp

    # Instantiate the cache
    ds = cached_func(agg_days=7)
    #  Cache should be disabled - these should be different random numbers
    dsp = cached_func(agg_days=7)
    assert ds != dsp

    # Retest with agg days 1
    ds = cached_func(agg_days=1)
    #  Cache should be disabled - these should be different random numbers
    dsp = cached_func(agg_days=1)
    assert ds == dsp


def test_cache_local():
    """Test that cache_local argument works."""
    @cacheable(data_type='basic',
               cache_args=['agg_days'],
               cache_local=True)
    def cached_func_393(agg_days=7):  # noqa: ARG001
        return np.random.randint(1000)

    local_path = os.path.expanduser("~/.cache/sheerwater/caches/cached_func_393/1.pkl")
    local_verify = os.path.expanduser("~/.cache/sheerwater/caches/cached_func_393/1.verify")
    if os.path.exists(local_path):
        os.remove(local_path)
    if os.path.exists(local_verify):
        os.remove(local_verify)

    # Run for the firs time
    ds1 = cached_func_393(agg_days=1, cache_local=True)
    assert os.path.exists(local_path)
    assert os.path.exists(local_verify)

    # Run again to ensure you hit the cache
    ds2 = cached_func_393(agg_days=1)
    assert ds1 == ds2

    # Run again, but hit the remote
    # Delete the local cache, shouldn't impact anything
    os.remove(local_path)
    os.remove(local_verify)
    ds3 = cached_func_393(agg_days=1, cache_local=False)
    assert not os.path.exists(local_path)
    assert not os.path.exists(local_verify)
    assert ds1 == ds3

    # Run again, with local true, should copy remote cache to local
    ds4 = cached_func_393(agg_days=1, cache_local=True)
    assert os.path.exists(local_path)
    assert os.path.exists(local_verify)
    assert ds1 == ds4

    # Now, corrupt the local cache, by making it out of sync with the remote
    fs = fsspec.filesystem('file')
    before_local_verify = fs.open(local_verify, 'r').read()
    fs.open(local_verify, 'w').write(
        datetime.datetime.now(datetime.timezone.utc).isoformat())
    fs.open(local_path, 'w').write(str(np.random.randint(1000)))
    ds5 = cached_func_393(agg_days=1, cache_local=True)
    assert ds1 == ds5
    # Check that the local verify has been restored to the remote verify
    assert fs.open(local_verify, 'r').read() == before_local_verify


def test_cache_local_recursive():
    """Test that cache_local argument works for a recursive parquet."""
    @cacheable(data_type='tabular', cache_args=['name'])
    def tab(name='bob'):
        """Test function for tabular data."""
        import pandas as pd

        data = [[name, np.random.randint(1000)], ['nick', np.random.randint(1000)], ['juli', np.random.randint(1000)]]
        df = pd.DataFrame(data, columns=['Name', 'Age'])
        return df

    # Get a random name
    name = ''.join(random.choices(string.ascii_letters, k=10))

    local_path = os.path.expanduser("~/.cache/sheerwater/caches/tab/{}.parquet".format(name))
    local_verify = os.path.expanduser("~/.cache/sheerwater/caches/tab/{}.verify".format(name))
    # Ensure no local before testing
    if os.path.exists(local_path):
        os.remove(local_path)
    if os.path.exists(local_verify):
        os.remove(local_verify)

    # Path 1: Run remote
    df = tab(name=name, backend='parquet').compute()
    assert not os.path.exists(local_path)

    # Path 2: Run local
    df2 = tab(name=name, backend='parquet', cache_local=True).compute()
    assert os.path.exists(local_path)
    assert os.path.exists(local_verify)
    assert df.equals(df2)

    # Path 3: Re-run both
    df3 = tab(name=name, backend='parquet',
              recompute=True, force_overwrite=True).compute()
    assert not df3.equals(df)
    df4 = tab(name=name, backend='parquet', cache_local=True).compute()
    assert not df4.equals(df2)
    assert df4.equals(df3)


if __name__ == "__main__":
    test_null_time_caching()
    test_validate_timeseries()
    test_tabular_timeseries()
    test_cache_disable_if()
    test_cache_arg_scope()
