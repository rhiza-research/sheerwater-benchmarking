"""Mask data objects."""
import os
import cdsapi
import xarray as xr

from sheerwater_benchmarking.utils import cacheable

from sheerwater_benchmarking.utils.secrets import cdsapi_secret
from sheerwater_benchmarking.utils.data_utils import get_grid


@cacheable(data_type='array', cache_args=['grid'])
def land_sea_mask(grid="global1_5"):
    """Get the ECMWF land sea mask for the given grid.

    Args:
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree 
    """
    times = ['00:00']
    days = ['01']
    months = ['01']
    _, _, grid_size = get_grid(grid)

    # Make sure the temp folder exists
    os.makedirs('./temp', exist_ok=True)
    path = "./temp/lsm.nc"

    # Create a file path in the temp folder
    url, key = cdsapi_secret()
    c = cdsapi.Client(url=url, key=key)
    c.retrieve('reanalysis-era5-single-levels',
               {
                   'product_type': 'reanalysis',
                   'variable': "land_sea_mask",
                   'year': 2022,
                   'month': months,
                   'day': days,
                   'time': times,
                   'format': 'netcdf',
                   'grid': [str(grid_size), str(grid_size)],
               },
               path)

    # Some transformations
    ds = xr.open_dataset(path)
    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon', 'lsm': 'mask'})
    ds = ds.sel(time=ds['time'].values[0])
    ds = ds.drop('time')

    ds = ds.compute()
    os.remove(path)
    return ds
