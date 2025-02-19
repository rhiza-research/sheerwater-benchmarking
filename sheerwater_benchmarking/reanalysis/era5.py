"""Fetches ERA5 data from the Google ARCO Store."""
import xarray as xr
import numpy as np
from functools import partial
from sheerwater_benchmarking.utils import (dask_remote, cacheable,
                                           get_variable, apply_mask, clip_region,
                                           roll_and_agg, lon_base_change, regrid)
from sheerwater_benchmarking.tasks import spw_rainy_onset, spw_precip_preprocess


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
           cache_args=['variable', 'grid'],
           cache_disable_if={'grid': 'global0_25'},
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               }
           },
           auto_rechunk=False)
def era5_daily_regrid(start_time, end_time, variable, grid="global0_25"):
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
    ds = regrid(ds, grid, base='base180', method='conservative', output_chunks={"lat": 121, "lon": 240})
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'agg_days', 'grid'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           cache_disable_if={'agg_days': 1},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               }
           })
def era5_rolled(start_time, end_time, variable, agg_days=7, grid="global1_5"):
    """Aggregates the hourly ERA5 data into daily data and rolls.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        agg_days (int): The aggregation period, in days.
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
            - global0_25: 0.25 degree global grid
    """
    # Read and combine all the data into an array
    ds = era5_daily_regrid(start_time, end_time, variable, grid=grid)
    if agg_days == 1:
        return ds
    ds = roll_and_agg(ds, agg=agg_days, agg_col="time", agg_fn="mean")
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'agg_days', 'grid', 'mask', 'region'])
def era5(start_time, end_time, variable, agg_days, grid='global0_25', mask='lsm', region='global'):
    """Standard format task data for ERA5 Reanalysis.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        agg_days (int): The aggregation period, in days. Ignored if variable is 'rainy_onset'.
        grid (str): The grid resolution to fetch the data at.
        mask (str): The mask to apply to the data.
        region (str): The region to clip the data to.
    """
    # Get daily data
    if variable == 'rainy_onset' or variable == 'rainy_onset_no_drought':
        drought_condition = (variable == 'rainy_onset_no_drought')
        fn = partial(era5_rolled, start_time, end_time, variable='precip', grid=grid)
        roll_days = [8, 11] if not drought_condition else [8, 11, 11]
        shift_days = [0, 0] if not drought_condition else [0, 0, 11]
        data = spw_precip_preprocess(fn, agg_days=roll_days, shift_days=shift_days,
                                     mask=mask, region=region, grid=grid)
        ds = spw_rainy_onset(data,
                             onset_group=['ea_rainy_season', 'year'], aggregate_group=None,
                             time_dim='time', prob_type='deterministic',
                             drought_condition=drought_condition,
                             mask=mask, region=region, grid=grid)
        # Rainy onset is sparse, so we need to set the sparse attribute
        ds = ds.assign_attrs(sparse=True)
    else:
        ds = era5_rolled(start_time, end_time, variable, agg_days=agg_days, grid=grid)
        # Apply masking and clip region
        ds = apply_mask(ds, mask, grid=grid)
        ds = clip_region(ds, region=region)
    return ds
