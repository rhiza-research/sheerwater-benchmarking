"""Interface for graphcast forecasts."""
import os
import dask
import xarray as xr
import numpy as np
import pandas as pd
import dateparser

from sheerwater_benchmarking.utils import (dask_remote, cacheable,
                                           apply_mask, clip_region,
                                           lon_base_change,
                                           target_date_to_forecast_date,
                                           shift_forecast_date_to_target_date, lead_to_agg_days, roll_and_agg, regrid)

@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid'],
           timeseries='time',
           cache=True,
           cache_disable_if={'grid': 'global0_25'},
           validate_cache_timeseries=False)
def graphcast_daily(start_time, end_time, variable, grid='global0_25'):
    "graphcast Daily."
    # Read the three years for gcloud
    ds1 = xr.open_zarr('gs://weathernext/59572747_4_0/zarr/99140631_1_2020_to_2021/forecasts_10d/date_range_2019-12-01_2021-01-22_6_hours.zarr/')
    ds1 = ds1.rename({'prediction_timedelta': 'lead_time', '2m_temperature': 'tmp2m', 'total_precipitation_6hr' : 'precip'})
    ds1 = ds1[[variable]]
    ds1 = ds1.sel({'time': slice(pd.to_datetime("2019-12-01"), pd.to_datetime("2020-11-30"))})

    ds2 = xr.open_zarr('gs://weathernext/59572747_4_0/zarr/99140631_2_2021_to_2022/forecasts_10d/date_range_2020-12-01_2022-01-22_6_hours.zarr')
    ds2 = ds2.rename({'prediction_timedelta': 'lead_time', '2m_temperature': 'tmp2m', 'total_precipitation_6hr' : 'precip'})
    ds2 = ds2[[variable]]
    ds2 = ds2.sel({'time': slice(pd.to_datetime("2020-12-01"), pd.to_datetime("2021-12-31"))})

    ds3 = xr.open_zarr('gs://weathernext/59572747_4_0/zarr/132880704_2022_to_2023/1/forecasts_10d/date_range_2022-01-01_2023-01-01_12_hours.zarr')
    ds3 = ds3.rename({'prediction_timedelta': 'lead_time', '2m_temperature': 'tmp2m', 'total_precipitation_6hr' : 'precip'})
    ds3 = ds3[[variable]]
    ds3 = ds3.sel({'time': slice(pd.to_datetime("2022-01-01"), pd.to_datetime("2023-01-01"))})

    ## concat them together
    ds = xr.concat([ds1, ds2, ds3], 'time')

    # Aggregate into daily bins
    if variable == 'precip':
        ds = ds.resample(lead_time='1D').sum()
    else:
        ds = ds.resample(lead_time='1D').mean()

    # Shift the lead back 6 hours to be aligned
    ds['lead_time'] = ds['lead_time'] - np.timedelta64(6, 'h')
    dates = pd.date_range("2019-12-01", "2022-12-31")
    dates = [pd.to_datetime(date) for date in dates]

    ds = ds.sel(time=dates)

    # Convert lat/lon
    ds = lon_base_change(ds)
    ds = ds.sortby(ds.lat)

    # Regrid
    if grid != 'global0_25':
        ds = regrid(ds, grid, base='base180', method='conservative',
                    output_chunks={"lat": 721, "lon": 1440})

    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'agg_days', 'grid'],
           timeseries='time',
           cache=True,
           validate_cache_timeseries=False)
def graphcast_rolled(start_time, end_time, variable, agg_days, grid='global0_25'):
    """ graphcast Rolled. """
    ds = graphcast_daily(start_time, end_time, variable, grid)

    ds = roll_and_agg(ds, agg=agg_days, agg_col="lead_time", agg_fn="mean")

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def graphcast(start_time, end_time, variable, lead, prob_type='deterministic',
         grid='global1_5', mask='lsm', region="global"):
    """Final graphcast interface"""
    if prob_type != 'deterministic':
        raise NotImplementedError("Only deterministic forecast implemented for graphcast")

    """Standard format forecast data for daily forecasts."""
    lead_params = {
        "week1": ('weekly', 0),
    }

    time_group, lead_offset_days = lead_params.get(lead, (None, None))
    if time_group is None:
        raise NotImplementedError(f"Lead {lead} not implemented for graphcast forecasts.")

    # Convert start and end time to forecast start and end based on lead time
    forecast_start = target_date_to_forecast_date(start_time, lead)
    forecast_end = target_date_to_forecast_date(end_time, lead)

    # Get the data with the right days
    agg_days = lead_to_agg_days(lead)
    ds = graphcast_rolled(start_time, end_time, variable, agg_days=agg_days, grid=grid)
    ds = ds.assign_attrs(prob_type="deterministic")

    # Get specific lead
    lead_shift = np.timedelta64(lead_offset_days, 'D')
    ds = ds.sel(lead_time=lead_shift)

    # Time shift - we want target date, instead of forecast date
    ds = shift_forecast_date_to_target_date(ds, 'time', lead)

    # Apply masking and clip to region
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    ds = clip_region(ds, region=region)

    return ds
