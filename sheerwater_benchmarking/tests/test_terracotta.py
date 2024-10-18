from sheerwater_benchmarking.utils import cacheable
from sheerwater_benchmarking.reanalysis.era5 import era5

@cacheable(data_type='array', cache_args=['variable'])
def terra(variable='tmp2m'):
    ds = era5("2000-01-01 00:00:00", "2000-02-01 00:00:00", variable=variable, lead='week1')
    ds = ds.mean('time')

    return ds

def test_terra():

    # Call tabular to cache it
    terra('tmp2m', backend='terracotta')
