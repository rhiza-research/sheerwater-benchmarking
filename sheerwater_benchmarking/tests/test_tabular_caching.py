from sheerwater_benchmarking.utils import cacheable

@cacheable(data_type='tabular', cache_args=['name'])
def tab(name='bob'):

    import pandas as pd

    data = [[name, 10], ['nick', 15], ['juli', 14]]
    df = pd.DataFrame(data, columns=['Name', 'Age'])
    return df

def test_tab():

    # Call tabular to cache it
    tab('josh')
