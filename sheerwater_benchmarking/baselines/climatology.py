"""A climatology baseline for benchmarking."""
import dateparser
from datetime import datetime, timedelta
import numpy as np
import xarray as xr
import dask

from sheerwater_benchmarking.reanalysis import era5_agg
from sheerwater_benchmarking.masks import land_sea_mask
from sheerwater_benchmarking.utils import (dask_remote, cacheable, roll_and_agg,
                                           get_dates, apply_mask, clip_region)


@dask_remote
@cacheable(data_type='array',
           cache_args=['first_year', 'last_year', 'variable', 'grid', 'mask'],
           chunking={"lat": 121, "lon": 240, "dayofyear": 366},
           auto_rechunk=False)
def climatology(first_year, last_year, variable, grid="global1_5", mask="lsm"):
    """Compute the climatology of the ERA5 data. Years are inclusive."""
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year}-12-31"

    # Get single day, masked data between start and end years
    ds = era5_agg(start_time, end_time, variable=variable,
                  grid=grid, agg=1, mask=mask)

    # Add day of year as a coordinate
    ds = ds.assign_coords(dayofyear=ds.time.dt.dayofyear)

    # Take average over the period to produce climatology
    return ds.groupby('dayofyear').mean(dim='time')


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid', 'mask'],
           chunking={"lat": 121, "lon": 240, "dayofyear": 366},
           cache=False,
           auto_rechunk=False)
def climatology_standard_30yr(variable, grid="global1_5", mask="lsm"):
    """Compute the standard 30-year climatology of ERA5 data from 1991-2020."""
    # Get single day, masked data between start and end years
    return climatology(1991, 2020, variable, grid=grid, mask=mask)


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=True,
           cache_args=['variable', 'first_year', 'last_year', 'agg', 'grid', 'mask'],
           chunking={"lat": 721, "lon": 1441, "time": 30},
           auto_rechunk=False)
def climatology_agg(start_time, end_time, variable, first_year=1991, last_year=2020,
                    agg=14, grid="global1_5", mask="lsm"):
    """Generates a daily timeseries of climatology with rolling aggregation.

    Args:
        start_time (str): The start time of the timeseries forecast.
        end_time (str): The end time of the timeseries forecast.
        variable (str): The weather variable to fetch.
        grid (str): The grid to produce the forecast on.
        agg (str): The aggregation period to use, in days
        mask (str): The mask to apply to the data. One of:
            - lsm: Land-sea mask
            - None: No mask
    """
    # Get climatology on the corresponding global grid
    clim = climatology(first_year=first_year, last_year=last_year, variable=variable,
                       grid=grid, mask=mask)

    # Create a target date dataset
    target_dates = get_dates(start_time, end_time, stride='day', return_string=False)
    time_ds = xr.Dataset({'time': target_dates})
    time_ds = time_ds.assign_coords(dayofyear=time_ds['time'].dt.dayofyear)

    # Select the climatology data for the target dates, and split large chunks
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        ds = clim.sel(dayofyear=time_ds.dayofyear)
        ds = ds.drop('dayofyear')

    # Roll and aggregate the data
    agg_fn = "sum" if variable == "precip" else "mean"
    ds = roll_and_agg(ds, agg=agg, agg_col="time", agg_fn=agg_fn)

    # Mask the result
    if mask == "lsm":
        # Select variables and apply mask
        mask_ds = land_sea_mask(grid=grid).compute()
    elif mask is None:
        mask_ds = None
    else:
        raise NotImplementedError("Only land-sea or None mask is implemented.")
    ds = apply_mask(ds, mask_ds, variable)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def climatology_forecast(start_time, end_time, variable, lead, prob_type='deterministic',
                         grid='global0_25', mask='lsm', region='africa'):
    """Standard format forecast data for climatology forecast."""
    leads_param = {
        "week1": (7, 0),
        "week2": (7, 7),
        "week3": (7, 14),
        "week4": (7, 21),
        "week5": (7, 28),
        "week6": (7, 36),
        "weeks12": (14, 0),
        "weeks23": (14, 7),
        "weeks34": (14, 14),
        "weeks45": (14, 21),
        "weeks56": (14, 28),
    }

    agg, time_shift = leads_param[lead]

    # Get daily data
    new_start = datetime.strftime(dateparser.parse(start_time)+timedelta(days=time_shift), "%Y-%m-%d")
    new_end = datetime.strftime(dateparser.parse(end_time)+timedelta(days=time_shift), "%Y-%m-%d")
    ds = climatology_agg(new_start, new_end, variable, agg=agg, grid=grid, mask=mask)
    ds = ds.assign_coords(time=ds['time']-np.timedelta64(time_shift, 'D'))

    if prob_type == 'deterministic':
        ds = ds.assign_coords(member=-1)
    else:
        raise NotImplementedError("Only deterministic forecasts are available for climatology.")
    # Clip to region
    ds = clip_region(ds, region)
    return ds
