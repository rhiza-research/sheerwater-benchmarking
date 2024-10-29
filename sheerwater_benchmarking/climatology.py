"""Climatology models."""
import dateparser
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
import xarray as xr
from sheerwater_benchmarking.reanalysis import era5_daily, era5_rolled
from sheerwater_benchmarking.utils import (dask_remote, cacheable, apply_mask, clip_region, roll_and_agg)


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'first_year', 'last_year', 'grid'],
           chunking={"lat": 721, "lon": 1440, "dayofyear": 366},
           auto_rechunk=False)
def climatology_raw(variable, first_year=1985, last_year=2014, grid='global1_5'):
    """Compute the climatology of the ERA5 data. Years are inclusive."""
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year+1}-01-01"

    # Get single day, masked data between start and end years
    ds = era5_daily(start_time, end_time, variable=variable, grid=grid)

    # Add day of year as a coordinate
    ds = ds.assign_coords(dayofyear=ds.time.dt.dayofyear)

    # Take average over the period to produce climatology
    return ds.groupby('dayofyear').mean(dim='time')


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'first_year', 'last_year', 'grid', 'mask', 'region'],
           chunking={"lat": 721, "lon": 1440, "dayofyear": 366},
           cache=True)
def climatology(variable, first_year=1991, last_year=2020, grid="global1_5", mask='lsm', region='global'):
    """Compute the standard 30-year climatology of ERA5 data from 1991-2020."""
    # Get single day, masked data between start and end years
    ds = climatology_raw(variable, first_year, last_year, grid=grid)

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid', 'mask', 'region'],
           chunking={"lat": 721, "lon": 1440, "dayofyear": 366},
           cache=False,
           auto_rechunk=False)
def climatology_standard_30yr(variable, grid="global1_5", mask="lsm", region='global'):
    """Compute the standard 30-year climatology of ERA5 data from 1991-2020."""
    # Get single day, masked data between start and end years
    return climatology(variable, 1991, 2020, grid=grid, mask=mask, region=region)


@dask_remote
@cacheable(data_type='array',
           cache=True,
           cache_args=['variable', 'first_year', 'last_year', 'prob_type', 'agg', 'grid'],
           chunking={"lat": 121, "lon": 240, "doy": 30, "member": 30},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'doy': 1}
               }
           },
           auto_rechunk=False)
def climatology_agg(variable, first_year=1985, last_year=2014,
                    prob_type='deterministic', agg=14, grid="global1_5"):
    """Generates aggregated climatology."""
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year+1}-01-01"
    ds = era5_rolled(start_time, end_time, variable=variable, agg=agg, grid=grid)

    # Add day of year as a coordinate
    ds = ds.assign_coords(dayofyear=ds.time.dt.dayofyear)

    # Take average over the period to produce climatology
    if prob_type == 'deterministic':
        return ds.groupby('dayofyear').mean(dim='time')
    elif prob_type != 'probabilistic':
        raise ValueError(f"Unsupported prob_type: {prob_type}")
    # Otherwise, get ensemble members sampled from climatology

    def sample_members(sub_ds, members=100):
        doy = sub_ds.dayofyear.values[0]
        ind = np.random.randint(0, len(sub_ds.time.values), size=(members,))
        sub = sub_ds.isel(time=ind)
        sub = sub.assign_coords(time=np.arange(members)).rename({'time': 'member'})
        sub = sub.assign_coords(dayofyear=doy)
        return sub

    doys = []
    for doy in range(1, 367):
        doys.append(
            sample_members(ds.isel(time=(ds.dayofyear.values == doy))))
    ds = xr.concat(doys, dim='dayofyear')
    ds = ds.chunk({'dayofyear': 1, 'member': 30})
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'clim_years', 'agg', 'grid'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               }
           },
           cache=True)
def climatology_rolling_agg(start_time, end_time, variable, clim_years=30, agg=14, grid="global1_5"):
    """Compute a rolling {clim_years}-yr climatology of the ERA5 data.

    Args:
        start_time: First time of the forecast period.
        end_time: Last time of the forecast period.
        variable: Variable to compute climatology for.
        clim_years: Number of years to compute climatology over.
        grid: Grid resolution of the data.
    """
    #  Get reanalysis data for the appropriate look back period
    # We need data from clim_years before the start_time until 1 year before the end_time
    # as this climatology excludes the most recent year for use in operational forecasting
    new_start = (dateparser.parse(start_time) - relativedelta(years=clim_years)).strftime("%Y-%m-%d")
    new_end = (dateparser.parse(end_time) - relativedelta(years=1)).strftime("%Y-%m-%d")

    # Get ERA5 data, and ignore cache validation if start_time is earlier than the cache
    ds = era5_rolled(new_start, new_end, variable=variable, agg=agg, grid=grid)
    ds = ds.assign_coords(dayofyear=ds.time.dt.dayofyear)

    def doy_rolling(sub_ds, years):
        return sub_ds.rolling(time=years, min_periods=years, center=False).mean()

    # Rechunk the data to have a single time chunk for efficient rolling
    ds = ds.chunk(time=1)
    ds = ds.groupby('dayofyear').map(doy_rolling, years=clim_years)
    ds = ds.dropna('time', how='all')

    # Ground truth for the current time is not available at forecast time,
    # so we must shift the time index forward one year to provide climatology that
    # goes up until the year ~before~ the forecast date value, e.g,.
    # the climatology for forecast date 2016-01-01 is computed up until 2015-01-01.
    ds = ds.assign_coords(time=ds['time'].to_index() + pd.DateOffset(years=1))
    ds = ds.drop('dayofyear')
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='forecast_date',
           cache_args=['variable', 'clim_years', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               }
           },
           cache=False)
def climatology_rolling(start_time, end_time, variable, clim_years=30,
                        grid="global1_5", mask='lsm', region='global'):
    """Compute a rolling {clim_years}-yr climatology of the ERA5 data.

    Args:
        start_time: First time of the forecast period.
        end_time: Last time of the forecast period.
        variable: Variable to compute climatology for.
        clim_years: Number of years to compute climatology over.
        grid: Grid resolution of the data.
        mask: Mask to apply to the data.
        region: Region to clip the data to.
    """
    ds = climatology_rolling_raw(start_time, end_time, variable, clim_years=clim_years, grid=grid)

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)
    return ds


# @dask_remote
# @cacheable(data_type='array',
#            timeseries='time',
#            cache=True,
#            cache_args=['variable', 'clim_years', 'prob_type', 'agg', 'grid'],
#            chunking={"lat": 121, "lon": 240, "time": 1000},
#            chunk_by_arg={
#                'grid': {
#                    'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
#                }
#            },
#            auto_rechunk=False)
# def climatology_rolling_wip(start_time, end_time, variable, clim_years=30,
#                             prob_type='deterministic', agg=14, grid="global1_5"):
#     """Generates a daily timeseries of climatology with rolling aggregation.

#     This climatology is valid as a baseline for all times.

#     Args:
#         start_time (str): The start time of the timeseries forecast.
#         end_time (str): The end time of the timeseries forecast.
#         variable (str): The weather variable to fetch.
#         clim_years (int): The number of years to use for the climatology.
#         agg (str): The aggregation period to use, in days
#         grid (str): The grid to produce the forecast on.
#     """
#     #  Get reanalysis data for the appropriate look back period
#     # We need data from clim_years before the start_time until 1 year before the end_time
#     # as this climatology excludes the most recent year for use in operational forecasting
#     new_start = (dateparser.parse(start_time) - relativedelta(years=clim_years)).strftime("%Y-%m-%d")

#     # Get ERA5 data, and ignore cache validation if start_time is earlier than the cache
#     ds = era5_rolled(new_start, end_time, variable=variable, agg=agg, grid=grid)
#     ds = ds.assign_coords(dayofyear=ds.time.dt.dayofyear)

#     if prob_type == 'deterministic':
#         def doy_rolling(sub_ds, years):
#             return sub_ds.rolling(time=years, min_periods=years, center=False).mean()
#     elif prob_type == 'probabilistic':
#         def doy_rolling(sub_ds, years):
#             def sample_members(sub_ds, members=100):
#                 doy = sub_ds.dayofyear.values[0]
#                 ind = np.random.randint(0, len(sub_ds.time.values), size=(members,))
#                 sub = sub_ds.isel(time=ind)
#                 sub = sub.assign_coords(time=np.arange(members)).rename({'time': 'member'})
#                 sub = sub.assign_coords(dayofyear=doy)
#                 return sub
#             return sub_ds.rolling(time=years, min_periods=years, center=False).map(sample_members)
#     else:
#         raise ValueError(f"Unsupported prob_type: {prob_type}")

#     # Rechunk the data to have a single time chunk for efficient rolling
#     ds = ds.chunk(time=1)
#     ds = ds.groupby('dayofyear').map(doy_rolling, years=clim_years)
#     ds = ds.dropna('time', how='all')

#     return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'first_year', 'last_year', 'agg', 'grid'],
           chunking={"lat": 721, "lon": 1440, "dayofyear": 366},
           cache=True,
           auto_rechunk=False)
def climatology_trend(variable, first_year=1985, last_year=2014, agg=14, grid='global1_5'):
    """Fit the climatological trend for a specific day of year.

    Args:
        variable: Variable to compute climatology for.
        first_year: First year of the climatology.
        last_year: Last year of the climatology.
        agg: Aggregation period in days.
        grid: Grid resolution of the data.
    """
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year+1}-01-01"

    # Get single day, masked data between start and end years
    ds = era5_rolled(start_time, end_time, variable=variable, agg=agg, grid=grid)

    # Add day of year as a coordinate
    ds = ds.assign_coords(dayofyear=ds.time.dt.dayofyear)
    ds = ds.assign_coords(year=ds.time.dt.year)

    def fit_trend(sub_ds):
        sub_ds = sub_ds.swap_dims({"time": "year"})
        return sub_ds.polyfit(dim='year', deg=1)

    # Fit the trend for each day of the year
    ds = ds.groupby('dayofyear').map(fit_trend)
    return ds


__all__ = ['climatology', 'climatology_standard_30yr', 'climatology_rolling', 'climatology_agg']
