"""Mask data objects."""
import os
import cdsapi
import xarray as xr
import numpy as np

from sheerwater_benchmarking.utils import (cacheable, cdsapi_secret, get_grid, clip_region,
                                           lon_base_change, get_region_labels, get_grid_ds)


@cacheable(data_type='array', cache_args=['grid'])
def land_sea_mask(grid="global1_5"):
    """Get the ECMWF global land sea mask for the given grid.

    Args:
        grid (str): The grid to fetch the data at.  Note that only
            the resolution of the specified grid is used.
    """
    _, _, grid_size = get_grid(grid, base="base360")

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

    # Convert to our standard base 180 format
    ds = lon_base_change(ds, to_base="base180")

    # Sort and select a subset of the data
    ds = ds.sortby(['lon', 'lat'])  # CDS API returns lat data in descending order, breaks slicing

    ds = ds.compute()
    os.remove(path)
    return ds


@cacheable(data_type='array',
           cache_args=['grid', 'admin_level'],
           chunking={'lat': 1000, 'lon': 1000})
def region_labels(grid='global1_5', admin_level='countries'):
    """Generate a dataset with a region coordinate at a specific admin level.

    Available admin levels are 'country', 'region', 'continent', and 'world'.

    Args:
        grid (str): The grid to fetch the data at.  Note that only
            the resolution of the specified grid is used.
        admin_level (str): The admin level to add to the dataset

    Returns:
        xarray.Dataset: Dataset with added region coordinate
    """
    # Get the list of regions for the specified admin level
    region_names = get_region_labels(admin_level)
    ds = get_grid_ds(grid)
    world_ds = xr.full_like(ds.lat * ds.lon, 1.0, dtype=np.float32)
    # Assign a dummy region coordinate to all grid cells
    ds = ds.assign_coords(region=(('lat', 'lon'), xr.full_like(ds.lat * ds.lon, 'no_region', dtype=object).data))

    # Loop through each region and label grid cells
    for i, rn in enumerate(region_names):
        print(i, '/', len(region_names), rn)
        # Clip dataset to this region
        region_ds = clip_region(world_ds, rn, keep_shape=True)
        # Create a mask where the region exists (non-NaN values)
        region_mask = ~region_ds.isnull()
        # Assign region name where the mask is True
        ds['region'] = ds.region.where(~region_mask, rn)
    return ds


@cacheable(data_type='array',
           cache_args=['grid'],
           chunking={'lat': 1000, 'lon': 1000})
def region_mask(grid='global1_5'):
    """Generate a dataset with a region coordinate at a specific admin level.

    Args:
        grid (str): The grid to fetch the data at.  Note that only
        admin_level (str): The admin level to add to the dataset

    Returns:
        xarray.Dataset: Dataset with added region coordinate
    """
    # Get the list of regions for the specified admin level
    region_names = get_region_labels('country')
    ds = get_grid_ds(grid)
    world_ds = xr.full_like(ds.lat * ds.lon, 1.0, dtype=np.float32)
    region_masks = []

    # Loop through each region and label grid cells
    for i, rn in enumerate(region_names):
        # Clip dataset to this region
        region_ds = clip_region(world_ds, rn, keep_shape=True)
        # Create a mask where the region exists (non-NaN values)
        region_mask = ~region_ds.isnull()
        region_masks.append(region_mask)
        if i > 5:
            break

    mask = xr.concat(region_masks, dim='region')
    mask = mask.assign_coords(region=region_names[:len(region_masks)])
    return mask


# Use __all__ to define what is part of the public API.
__all__ = [land_sea_mask]
