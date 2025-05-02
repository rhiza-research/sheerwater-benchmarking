"""Test cacheable upsert functionality."""
import numpy as np
import dask.dataframe as dd
from sheerwater_benchmarking.utils import cacheable, get_dates

@cacheable(data_type='tabular',
          #timeseries='time',
           backend='parquet',
           primary_keys=['time', 'obs'],
           cache_args=[])
def tabular_timeseries(random):  # noqa: ARG001
    """Generate a simple tabular timeseries dataset for testing.

    The 'species' argument is unused, but still matters because it's a cache key
    """
    start_time = "2024-01-01"
    end_time = "2024-01-10"
    times = get_dates(start_time, end_time, stride='day', return_string=False)
    obs = list(range(0, len(times)))

    #add an observation based on the argument
    obs.append(random)
    time = get_dates("2024-01-11", "2024-01-11", stride='day', return_string=False)
    times += time

    ds = dd.from_dict({'obs': obs, 'time': times}, npartitions=1)
    return ds

def test_upsert():
    """Test upsert on parquet."""
    # Call to make sure it is in place and count rows
    # should have some number of rows
    df = tabular_timeseries(0)
    scount = df.shape[0].compute()
    print(scount)

    # Call with a random int - should add a row
    df = tabular_timeseries(0, upsert=True)
    samecount = df.shape[0].compute()
    print(samecount)
    assert scount == samecount

    # Call with a random int - should add a row
    r = np.random.randint(10,1000000)
    df = tabular_timeseries(r, upsert=True)
    ncount = df.shape[0].compute()
    print(ncount)

    # Call with the same random int - should not add a row
    df = tabular_timeseries(r, upsert=True)
    npcount = df.shape[0].compute()
    print(npcount)

    assert ncount == npcount
    assert ncount == (scount + 1)

def test_postgres_upsert():
    """Test upsert on postgres."""
    # Call to make sure it is in place and count rows
    # should have some number of rows
    df = tabular_timeseries(0, backend='postgres')

    # read
    df = tabular_timeseries(0, backend='postgres')
    scount = df.shape[0]
    print(scount)

    # Call same with upsert
    df = tabular_timeseries(0, backend='postgres', upsert=True)

    # read
    df = tabular_timeseries(0, backend='postgres')
    samecount = df.shape[0]
    print(samecount)
    assert scount == samecount

    # Call with a random int - should add a row
    r = np.random.randint(10,1000000)
    df = tabular_timeseries(r, backend='postgres', upsert=True)

    # read
    df = tabular_timeseries(0, backend='postgres')
    ncount = df.shape[0]
    print(ncount)

    # Call with the same random int - should not add a row
    df = tabular_timeseries(r, backend='postgres', upsert=True)

    # read
    df = tabular_timeseries(0, backend='postgres')
    npcount = df.shape[0]
    print(npcount)

    assert ncount == npcount
    assert ncount == (scount + 1)


if __name__ == "__main__":
    test_upsert()
    test_postgres_upsert()
