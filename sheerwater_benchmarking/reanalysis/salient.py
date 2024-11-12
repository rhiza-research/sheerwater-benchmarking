from datetime import datetime, timedelta
import dateparser

import numpy as np
import xarray as xr

import salientsdk as sk

from sheerwater_benchmarking.utils import (cacheable, dask_remote, salient_secret,
                                           apply_mask, clip_region, roll_and_agg)


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'grid'],
           chunking={"lat": 300, "lon": 400, "time": 300},
           auto_rechunk=False)
def salient_era5_raw(start_time, end_time, variable, grid="salient0_25"):  # noqa: ARG001
    """Processed Salient forecast files."""
    sk.set_file_destination("/Users/avocet/content/sheerwater-benchmarking/salient_scripts/validation_example")
    username, password = salient_secret()
    sk.login(username, password)

    loc = sk.Location(shapefile=sk.upload_shapefile(
        coords=[(-26, -35), (73, -35), (73, 38), (-26, 38)],
        geoname="all_africa",  # the full African continent
        force=True))

    var = {"precip": "precip", "tmp2m": "temp"}[variable]

    hist = sk.data_timeseries(
        loc=loc,
        variable=var,
        field="vals",
        start=np.datetime64(start_time) - np.timedelta64(5, "D"),
        end=np.datetime64(end_time) + np.timedelta64(40, "D"),
        frequency="daily",
        verbose=True,
        force=False,
    )

    ds = xr.load_dataset(hist)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'agg', 'grid'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               }
           })
def salient_era5_rolled(start_time, end_time, variable, agg=14, grid="global1_5"):
    """Aggregates the hourly ERA5 data into daily data and rolls.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        agg (str): The aggregation period to use, in days
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
            - global0_25: 0.25 degree global grid
    """
    # Read and combine all the data into an array
    ds = salient_era5_raw(start_time, end_time, variable, grid=grid)
    agg_fn = "mean"  # all variables are rates, so we take the mean
    ds = roll_and_agg(ds, agg=agg, agg_col="time", agg_fn=agg_fn)

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'grid', 'mask', 'region'])
def salient_era5(start_time, end_time, variable, lead, grid='salient0_25', mask='lsm', region='africa'):
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
    lead_params = {
        "week1": (7, 0),
        "week2": (7, 7),
        "week3": (7, 14),
        "week4": (7, 21),
        "week5": (7, 28),
        "week6": (7, 35),
        "weeks12": (14, 0),
        "weeks23": (14, 7),
        "weeks34": (14, 14),
        "weeks45": (14, 21),
        "weeks56": (14, 28),
    }

    agg, time_shift = lead_params.get(lead, (None, None))
    if time_shift is None:
        raise NotImplementedError(f"Lead {lead} not implemented for Salient ERA5.")
    if grid != "salient0_25":
        raise NotImplementedError(f"Grid {grid} not implemented for Salient ERA5.")

    # Get daily data
    new_start = datetime.strftime(dateparser.parse(start_time)+timedelta(days=time_shift), "%Y-%m-%d")
    new_end = datetime.strftime(dateparser.parse(end_time)+timedelta(days=time_shift), "%Y-%m-%d")
    ds = salient_era5_rolled(new_start, new_end, variable, agg=agg, grid=grid)
    ds = ds.assign_coords(time=ds['time']-np.timedelta64(time_shift, 'D'))

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)
    return ds
