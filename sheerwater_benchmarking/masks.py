"""Mask data objects."""
import os
import cdsapi
import xarray as xr

from sheerwater_benchmarking.utils import (cacheable, cdsapi_secret, get_grid,
                                           lon_base_change, get_globe_slice)


@cacheable(data_type='array', cache_args=['grid', 'base'])
def land_sea_mask(grid="global1_5", base="base180"):
    """Get the ECMWF land sea mask for the given grid.

    Args:
        grid (str): The grid to fetch the data at.  Note that only
            the resolution of the specified grid is used.
        base (str): The longitude base to return the data in. One of:
            - base180, base360
    """
    if base not in ["base180", "base360"]:
        raise ValueError("Base must be either 'base180' or 'base360'.")

    lons, lats, grid_size = get_grid(grid)

    # Get data from the CDS API
    times = ['00:00']
    days = ['01']
    months = ['01']

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
                   'area': [lats[0], lons[0], lats[-1], lons[-1]],
               },
               path)

    # Some transformations
    ds = xr.open_dataset(path)

    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon', 'lsm': 'mask'})

    if 'valid_time' in ds.coords:
        time_var = 'valid_time'
    elif 'time' in ds.coords:
        time_var = 'time'
    else:
        raise ValueError("Could not find time variable in dataset.")
    ds = ds.sel(**{time_var: ds[time_var].values[0]})
    ds = ds.drop(time_var)

    if 'expver' in ds.coords:
        ds = ds.drop('expver')

    # Sort and select a subset of the data
    ds = ds.sortby('lat')  # CDS API returns lat data in descending order, breaks slicing
    ds = get_globe_slice(ds, lons, lats, base="base360")

    # Convert to our standard base 180 format
    if base == "base180":
        ds = lon_base_change(ds, to_base="base180")

    ds = ds.compute()
    os.remove(path)
    return ds


# Use __all__ to define what is part of the public API.
__all__ = [land_sea_mask]
