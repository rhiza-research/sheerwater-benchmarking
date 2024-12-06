"""Fetches ERA5 data from the Google ARCO Store."""
import xarray as xr
import numpy as np

from sheerwater_benchmarking.utils import (dask_remote, cacheable,
                                           get_variable, apply_mask, clip_region,
                                           roll_and_agg, lon_base_change, regrid)


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid'],
           timeseries='time',
           cache=False)
def era5_raw(start_time, end_time, variable, grid="global0_25"):  # noqa ARG001
    """ERA5 function that returns data from Google ARCO."""
    if grid != 'global0_25':
        raise NotImplementedError("Only ERA5 native 0.25 degree grid is implemented.")

    # Pull the google dataset
    ds = xr.open_zarr('gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3',
                      chunks={'time': 50, 'latitude': 721, 'longitude': 1440})

    # Select the right variable
    if variable in ['tmax2m', 'tmin2m']:
        var = 'tmp2m'  # Compute min and max daily temperatures from 2m temperature
    var = get_variable(variable, 'era5')
    ds = ds[var].to_dataset()

    # Convert local dataset naming and units
    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon'})

    # Raw latitudes are in descending order
    ds = ds.sortby('lat')
    ds = ds.rename_vars(name_dict={var: variable})

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'grid'],
           chunking={"lat": 721, "lon": 1440, "time": 30},
           auto_rechunk=False)
def era5_daily(start_time, end_time, variable, grid="global1_5"):
    """Aggregates the hourly ERA5 data into daily data.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
            - global0_25: 0.25 degree global grid
    """
    if grid != 'global0_25':
        raise ValueError("Only ERA5 native 0.25 degree grid is implemented.")

    # Read and combine all the data into an array
    ds = era5_raw(start_time, end_time, variable, grid='global0_25')

    # Convert to base180 longitude
    ds = lon_base_change(ds, to_base="base180")

    K_const = 273.15
    if variable == 'tmp2m':
        ds[variable] = ds[variable] - K_const
        ds.attrs.update(units='C')
        ds = ds.resample(time='D').mean(dim='time')
    if variable == 'tmax2m':
        ds[variable] = ds[variable] - K_const
        ds.attrs.update(units='C')
        ds = ds.resample(time='D').max(dim='time')
    if variable == 'tmin2m':
        ds[variable] = ds[variable] - K_const
        ds.attrs.update(units='C')
        ds = ds.resample(time='D').min(dim='time')
    elif variable == 'precip':
        ds[variable] = ds[variable] * 1000.0
        ds.attrs.update(units='mm')
        ds = ds.resample(time='D').sum(dim='time')
        # Can't have precip less than zero (there are some very small negative values)
        ds = np.maximum(ds, 0)
    elif variable == 'ssrd':
        ds = ds.resample(time='D').sum(dim='time')
        ds = np.maximum(ds, 0)
    else:
        raise ValueError(f"Variable {variable} not implemented.")
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'method', 'grid'],
           cache_disable_if={'grid': 'global0_25'},
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               }
           },
           auto_rechunk=False)
def era5_daily_regrid(start_time, end_time, variable, method="conservative", grid="global0_25"):
    """ERA5 daily reanalysis with regridding."""
    ds = era5_daily(start_time, end_time, variable, grid='global0_25')
    ds = ds.sortby('lat')  # TODO: remove if we fix the era5 daily caches
    if grid == 'global0_25':
        return ds

    # Regrid onto appropriate grid
    # Need all lats / lons in a single chunk to be reasonable
    chunks = {'lat': 721, 'lon': 1440, 'time': 30}
    ds = ds.chunk(chunks)
    # Need all lats / lons in a single chunk for the output to be reasonable
    ds = regrid(ds, grid, base='base180', method=method, output_chunks={"lat": 121, "lon": 240})
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'time_group', 'method', 'grid'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           cache_disable_if={'time_group': 'daily'},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               }
           })
def era5_rolled(start_time, end_time, variable, time_group='weekly', method='conservative', grid="global1_5"):
    """Aggregates the hourly ERA5 data into daily data and rolls.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        time_group (str): The aggregation period. One of: 'weekly', 'biweekly', 'monthly', 'quarterly'.
        method (str): The regridding method to use. One of: 'conservative', 'linear'
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
            - global0_25: 0.25 degree global grid
    """
    # Read and combine all the data into an array
    ds = era5_daily_regrid(start_time, end_time, variable, method=method, grid=grid)
    if time_group == 'daily':
        return ds

    agg = {'weekly': 7, 'biweekly': 14, 'monthly': 30, 'quarterly': 90}[time_group]
    ds = roll_and_agg(ds, agg=agg, agg_col="time", agg_fn="mean")
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'time_group', 'grid', 'mask', 'region'])
def era5(start_time, end_time, variable, time_group, grid='global0_25', mask='lsm', region='global'):
    """Standard format task data for ERA5 Reanalysis.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        lead (str): The lead time of the forecast.
        grid (str): The grid resolution to fetch the data at.
        mask (str): The mask to apply to the data.
        region (str): The region to clip the data to.
    """
    # Get daily data
    ds = era5_rolled(start_time, end_time, variable, time_group=time_group, method='conservative', grid=grid)
    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)
    return ds
