"""A climatology baseline forecast for benchmarking."""
import dateparser
from datetime import datetime, timedelta
import numpy as np
import xarray as xr
import dask

from sheerwater_benchmarking.climatology import climatology_rolling_raw, climatology_raw
from sheerwater_benchmarking.reanalysis import era5_rolled
from sheerwater_benchmarking.utils import (dask_remote, cacheable, roll_and_agg,
                                           get_dates, apply_mask, clip_region)


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=True,
           cache_args=['variable', 'clim_years', 'agg', 'grid'],
           chunking={"lat": 721, "lon": 1441, "time": 30},
           auto_rechunk=False)
def climatology_rolling_agg(start_time, end_time, variable, clim_years=30,
                            agg=14, grid="global1_5"):
    """Generates a daily timeseries of climatology with rolling aggregation.

    This climatology is valid as a baseline for all times.

    Args:
        start_time (str): The start time of the timeseries forecast.
        end_time (str): The end time of the timeseries forecast.
        variable (str): The weather variable to fetch.
        clim_years (int): The number of years to use for the climatology.
        agg (str): The aggregation period to use, in days
        grid (str): The grid to produce the forecast on.
    """
    # Get climatology on the corresponding grid
    ds = climatology_rolling_raw(start_time, end_time, variable=variable, clim_years=clim_years, grid=grid)
    ds = ds.rename({'forecast_date': 'time'})
    ds = ds.drop('dayofyear')

    # Roll and aggregate the data
    agg_fn = "sum" if variable == "precip" else "mean"
    ds = roll_and_agg(ds, agg=agg, agg_col="time", agg_fn=agg_fn)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def climatology_2015(start_time, end_time, variable, lead, prob_type='deterministic',
                     grid='global0_25', mask='lsm', region='global'):
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
    ds = climatology_agg(new_start, new_end, variable, first_year=1986, last_year=2015,
                         agg=agg, grid=grid)
    ds = ds.assign_coords(time=ds['time']-np.timedelta64(time_shift, 'D'))

    if prob_type != 'deterministic':
        raise NotImplementedError("Only deterministic forecasts are available for climatology.")
    ds = ds.assign_attrs(prob_type="deterministic")

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def climatology_rolling(start_time, end_time, variable, lead, prob_type='deterministic',
                        grid='global0_25', mask='lsm', region='global'):
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
    # TODO: need to ensure that this shifting doesn't grab data that's not available
    ds = climatology_rolling_agg(new_start, new_end, variable, clim_years=30, agg=agg, grid=grid)
    ds = ds.assign_coords(time=ds['time']-np.timedelta64(time_shift, 'D'))

    if prob_type != 'deterministic':
        raise NotImplementedError("Only deterministic forecasts are available for climatology.")
    ds = ds.assign_attrs(prob_type="deterministic")

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)
    return ds
