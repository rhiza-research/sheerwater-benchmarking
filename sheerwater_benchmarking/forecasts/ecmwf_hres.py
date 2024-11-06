"""Pulls ECMWF HRES forecasts from the WeatherBench2 bucket."""


import numpy as np
import xarray as xr

from sheerwater_benchmarking.masks import land_sea_mask
from sheerwater_benchmarking.utils import (cacheable, dask_remote,
                                           roll_and_agg,
                                           get_variable,
                                           apply_mask, clip_region,
                                           lon_base_change,
                                           regrid)


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid'],
           timeseries='time',
           cache=False)
def ecmwf_hres_raw(start_time, end_time, variable, grid="global0_25"):  # noqa ARG001
    """ECMWF function that returns data from Google Weather Bench."""
    if grid != "global0_25":
        raise NotImplementedError("Only ERA5 native 0.25 degree grid is implemented.")

    # Pull the google dataset
    ds = xr.open_zarr('gs://weatherbench2/datasets/hres/2016-2022-12h-6h-0p25deg-chunk-1.zarr',
                      chunks={'latitude': 721, 'longitude': 1440, 'time': 30, 'prediction_timedelta': 1})

    var = get_variable(variable, 'ecmwf_hres')
    # Select the right variable
    ds = ds[var].to_dataset()
    ds = ds.rename_vars(name_dict={var: variable})

    # Convert local dataset naming and units
    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon', 'prediction_timedelta': 'lead_time'})
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid'],
           chunking={"lat": 721, "lon": 1440, "lead_time": 1, "time": 30},
           timeseries='time')
def ecmwf_hres_daily(start_time, end_time, variable, grid="global0_25"):  # noqa ARG001
    """ECMWF function that returns data from Google Weather Bench."""
    if grid != "global0_25":
        raise NotImplementedError("Only ERA5 native 0.25 degree grid is implemented.")

    ds = ecmwf_hres_raw(start_time, end_time, variable, grid=grid)

    # Get the 00 issuance time and drop the 12 UTC issuance time
    ds = ds.sel(time=ds.time.dt.hour == 0)

    # Convert hourly data to daily data and then aggregate
    if variable == 'tmp2m':
        if ds[variable].units == 'K':
            ds[variable] = ds[variable] - 273.15
        ds = ds.resample(lead_time='D').mean(dim='lead_time')
    elif variable == 'precip':
        if ds[variable].units == 'm':
            ds[variable] = ds[variable] * 1000.0
        ds = ds.resample(lead_time='D').sum(dim='lead_time')
        ds = np.maximum(ds, 0)

    # Convert to base180 longitude
    ds = lon_base_change(ds, to_base="base180")
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'grid'],
           cache_disable_if={'grid': 'global0_25'},
           chunking={"lat": 121, "lon": 240, "lead_time": 1, "time": 1000},
           auto_rechunk=False)
def ecmwf_hres_daily_regrid(start_time, end_time, variable, grid="global0_25"):
    """ERA5 daily reanalysis with regridding."""
    ds = ecmwf_hres_daily(start_time, end_time, variable, grid='global0_25')
    if grid == 'global0_25':
        return ds
    # Regrid onto appropriate grid
    ds = regrid(ds, grid, base='base180')
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'agg', 'grid'],
           chunking={"lat": 121, "lon": 240, "time": 1000, "lead_time": 1},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30, "lead_time": 1},
               }
           })
def ecmwf_hres_rolled(start_time, end_time, variable, agg=14, grid="global1_5"):
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
    ds = ecmwf_hres_daily_regrid(start_time, end_time, variable, grid=grid)
    agg_fn = "sum" if variable == "precip" else "mean"
    ds = roll_and_agg(ds, agg=agg, agg_col="lead_time", agg_fn=agg_fn)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def ecmwf_hres(start_time, end_time, variable, lead, prob_type='deterministic',
               grid='global0_25', mask='lsm', region='global'):
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
    }

    agg, lead_id = lead_params.get(lead, (None, None))
    if agg is None:
        raise NotImplementedError(f"Lead {lead} not implemented for ECMWF HRES.")

    if prob_type == 'probabilistic':
        raise NotImplementedError("Probabilistic forecasts not implemented for ECMWF HRES.")

    # Get daily data
    ds = ecmwf_hres_rolled(start_time, end_time, variable, agg=agg, grid=grid)
    ds = ds.assign_attrs(prob_type="deterministic")

    # Get specific lead
    ds = ds.sel(lead_time=np.timedelta64(lead_id, 'D'))

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)
    return ds
