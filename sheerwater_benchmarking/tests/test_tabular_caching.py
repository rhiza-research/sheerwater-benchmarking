"""Test the postgres/tabular backend."""
from sheerwater_benchmarking.utils import cacheable

@cacheable(data_type='tabular', cache_args=['name'])
def tab(name='bob'):
    """Test function for tabular data."""
    import pandas as pd

    data = [[name, 10], ['nick', 15], ['juli', 14]]
    df = pd.DataFrame(data, columns=['Name', 'Age'])
    return df

@cacheable(data_type='tabular', cache_args=['name'])
def dtab(name='bob'):
    """Test function for tabular data."""
    import dask.dataframe as dd
    import pandas as pd

    data = [[name, 10], ['nick', 15], ['juli', 14]]
    df = pd.DataFrame(data, columns=['Name', 'Age'])
    df = dd.from_pandas(df)
    return df


def test_tab():
    """Test the tabular function."""
    # Call tabular to cache it
    tab('josh') # should cache
    tab('josh') # should return cache
    dtab('josh', backend='parquet') # should cache
    dtab('josh', backend='parquet') # should return cache
