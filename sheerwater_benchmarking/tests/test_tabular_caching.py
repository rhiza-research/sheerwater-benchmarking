"""Test the postgres/tabular backend."""
from sheerwater_benchmarking.utils import cacheable

@cacheable(data_type='tabular', cache_args=['name'])
def tab(name='bob'):
    """Test function for tabular data."""
    import pandas as pd

    data = [[name, 10], ['nick', 15], ['juli', 14]]
    df = pd.DataFrame(data, columns=['Name', 'Age'])
    return df

def test_tab():
    """Test the tabular function."""
    # Call tabular to cache it
    tab('josh')
