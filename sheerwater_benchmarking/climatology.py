"""Climatology models."""
import dateparser
import pandas as pd
from dateutil.relativedelta import relativedelta
from sheerwater_benchmarking.reanalysis import era5_daily
from sheerwater_benchmarking.utils import (dask_remote, cacheable, apply_mask, clip_region)


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'first_year', 'last_year', 'grid'],
           chunking={"lat": 721, "lon": 1440, "dayofyear": 366},
           auto_rechunk=False)
def climatology_raw(variable, first_year, last_year, grid='global1_5'):
    """Compute the climatology of the ERA5 data. Years are inclusive."""
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year}-12-31"

    # Get single day, masked data between start and end years
    ds = era5_daily(start_time, end_time, variable=variable, grid=grid)

    # Add day of year as a coordinate
    ds = ds.assign_coords(dayofyear=ds.time.dt.dayofyear)

    # Take average over the period to produce climatology
    return ds.groupby('dayofyear').mean(dim='time')


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'first_year', 'last_year', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "dayofyear": 366},
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
           chunking={"lat": 121, "lon": 240, "dayofyear": 366},
           cache=False,
           auto_rechunk=False)
def climatology_standard_30yr(variable, grid="global1_5", mask="lsm", region='global'):
    """Compute the standard 30-year climatology of ERA5 data from 1991-2020."""
    # Get single day, masked data between start and end years
    return climatology(variable, 1991, 2020, grid=grid, mask=mask, region=region)


@dask_remote
@cacheable(data_type='array',
           timeseries='forecast_date',
           cache_args=['variable', 'clim_years', 'grid'],
           chunking={"lat": 721, "lon": 1440, "time": 30},
           cache=True)
def climatology_rolling_raw(start_time, end_time, variable, clim_years=30, grid="global1_5"):
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
    ds = era5_daily(new_start, new_end, variable=variable, grid=grid)
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
    ds = ds.rename({'time': 'forecast_date'})
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='forecast_date',
           cache_args=['variable', 'clim_years', 'grid', 'mask', 'region'],
           chunking={"lat": 721, "lon": 1440, "time": 30},
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


__all__ = ['climatology', 'climatology_standard_30yr', 'climatology_rolling']
