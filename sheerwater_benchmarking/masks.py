"""Mask data objects."""
import os
import cdsapi
import geopandas as gpd
import xarray as xr

from sheerwater_benchmarking.utils import (cacheable, cdsapi_secret, get_grid, get_global_grid,
                                           lon_base_change, get_globe_slice, load_object)


@cacheable(data_type='array', cache_args=['grid'])
def land_sea_mask_global(grid="global1_5"):
    """Get the ECMWF global land sea mask for the given grid.

    Args:
        grid (str): The grid to fetch the data at.  Note that only
            the resolution of the specified grid is used.
    """
    lons, lats, grid_size, _ = get_grid(grid, base="base360")

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


@cacheable(data_type='array', cache_args=['grid'])
def land_sea_mask(grid="global1_5"):
    """Get the ECMWF land sea mask for the given grid.

    Args:
        grid (str): The grid to fetch the data at.
    """
    # Get global land sea mask
    ds = land_sea_mask_global(grid=get_global_grid(grid))

    # Select relevant subset
    lons, lats, _, region = get_grid(grid)
    ds = get_globe_slice(ds, lons, lats, base='base180')

    if region == 'africa':
        ds = clip_africa(ds)
    return ds


def clip_africa(ds, lon_dim='lon', lat_dim='lat'):
    """Clip a dataset to the African continent.

    Args:
        ds (xr.Dataset): The dataset to clip to Africa.
        lon_dim (str): The name of the longitude dimension.
        lat_dim (str): The name of the latitude dimension.
    """
    # Get the rectangle boundary of Africa
    ds = ds.rio.write_crs("EPSG:4326")
    ds = ds.rio.set_spatial_dims(lon_dim, lat_dim)

    # Get the countries of Africa shapefile
    filepath = 'gs://sheerwater-datalake/africa.geojson'
    gdf = gpd.read_file(load_object(filepath))

    # Clip the grid to the boundary of Africa
    ds = ds.rio.clip(gdf.geometry, gdf.crs, drop=False)

    return ds


# Use __all__ to define what is part of the public API.
__all__ = [land_sea_mask]
