"""Interface for graphcast forecasts."""
import xarray as xr
import numpy as np
import pandas as pd

from sheerwater_benchmarking.utils import (dask_remote, cacheable,
                                           apply_mask, clip_region,
                                           lon_base_change,
                                           target_date_to_forecast_date,
                                           shift_forecast_date_to_target_date,
                                           lead_to_agg_days, roll_and_agg, regrid, forecast)


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'init_hour', 'grid'],
           timeseries='time',
           chunking={"lat": 721, "lon": 1440, "lead_time": 1, "time": 30},
           validate_cache_timeseries=False)
def graphcast_daily(start_time, end_time, variable, init_hour=0, grid='global0_25'):  # noqa: ARG001
    """A daily Graphcast forecast."""
    if init_hour != 0:
        raise ValueError("Only 0 init hour supported.")

    # Read the three years for gcloud
    ds1 = xr.open_zarr(
        'gs://weathernext/59572747_4_0/zarr/99140631_1_2020_to_2021/forecasts_10d/date_range_2019-12-01_2021-01-22_6_hours.zarr',
        chunks='auto',
        decode_timedelta=True)
    ds1 = ds1.rename({'prediction_timedelta': 'lead_time',
                     '2m_temperature': 'tmp2m', 'total_precipitation_6hr': 'precip'})
    ds1 = ds1[[variable]]
    ds1 = ds1.sel({'time': slice(pd.to_datetime("2019-12-01"), pd.to_datetime("2020-11-30"))})

    ds2 = xr.open_zarr(
        'gs://weathernext/59572747_4_0/zarr/99140631_2_2021_to_2022/forecasts_10d/date_range_2020-12-01_2022-01-22_6_hours.zarr',
        decode_timedelta=True,
        chunks='auto')
    ds2 = ds2.rename({'prediction_timedelta': 'lead_time',
                     '2m_temperature': 'tmp2m', 'total_precipitation_6hr': 'precip'})
    ds2 = ds2[[variable]]
    ds2 = ds2.sel({'time': slice(pd.to_datetime("2020-12-01"), pd.to_datetime("2021-12-31"))})

    ds3 = xr.open_zarr(
        'gs://weathernext/59572747_4_0/zarr/132880704_2022_to_2023/1/forecasts_10d/date_range_2022-01-01_2023-01-01_12_hours.zarr',
        decode_timedelta=True,
        chunks='auto')
    ds3 = ds3.rename({'prediction_timedelta': 'lead_time',
                     '2m_temperature': 'tmp2m', 'total_precipitation_6hr': 'precip'})
    ds3 = ds3[[variable]]
    ds3 = ds3.sel({'time': slice(pd.to_datetime("2022-01-01"), pd.to_datetime("2023-01-01"))})

    # concat them together
    ds = xr.concat([ds1, ds2, ds3], dim='time')

    # Select only the midnight initialization times
    ds = ds.where(ds.time.dt.hour == init_hour, drop=True)

    # Convert units
    K_const = 273.15
    if variable == 'tmp2m':
        ds[variable] = ds[variable] - K_const
        ds.attrs.update(units='C')
        ds = ds.resample(lead_time='1D').mean(dim='lead_time')
    elif variable == 'precip':
        ds[variable] = ds[variable] * 1000.0
        ds.attrs.update(units='mm')
        # Convert from 6hrly to daily precip, robust to different numbers of 6hrly samples in a day
        ds = ds.resample(lead_time='1D').mean(dim='lead_time') * 4.0
        # Can't have precip less than zero (there are some very small negative values)
        ds = np.maximum(ds, 0)
    else:
        raise ValueError(f"Variable {variable} not implemented.")

    # Shift the lead back 6 hours + init time to align with the start date convention
    ds['lead_time'] = ds['lead_time'] - np.timedelta64(6+init_hour, 'h')

    # Convert lat/lon
    ds = lon_base_change(ds)

    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'init_hour', 'grid'],
           timeseries='time',
           chunking={"lat": 121, "lon": 240, "lead_time": 1, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'lead_time': 1, 'time': 30}
               },
           },
           validate_cache_timeseries=False)
def graphcast_daily_wb(start_time, end_time, variable, init_hour=0, grid='global0_25'):  # noqa: ARG001
    """A daily Graphcast forecast."""
    if init_hour != 0:
        raise ValueError("Only 0 init hour supported.")

    # Read the three years for gcloud
    if grid == 'global0_25':
        filename1 = 'gs://weatherbench2/datasets/graphcast/2018/date_range_2017-11-16_2019-02-01_12_hours.zarr'
        filename2 = 'gs://weatherbench2/datasets/graphcast/2020/date_range_2019-11-16_2021-02-01_12_hours.zarr'
    elif grid == 'global1_5':
        # Global 1.5 degree grid is fractionally off from the standard 1.5 grid
        filename1 = 'gs://weatherbench2/datasets/graphcast/2018/date_range_2017-11-16_2019-02-01_12_hours-240x121_equiangular_with_poles_conservative.zarr'
        filename2 = 'gs://weatherbench2/datasets/graphcast/2020/date_range_2019-11-16_2021-02-01_12_hours-240x121_equiangular_with_poles_conservative.zarr'
    else:
        raise ValueError(f"Grid {grid} not implemented.")

    ds1 = xr.open_zarr(filename1,
                       chunks='auto',
                       decode_timedelta=True)
    ds1 = ds1.rename({'prediction_timedelta': 'lead_time',
                     '2m_temperature': 'tmp2m', 'total_precipitation_6hr': 'precip'})
    if 'latitude' in ds1.dims:
        ds1 = ds1.rename({'latitude': 'lat', 'longitude': 'lon'})
    ds1 = ds1[[variable]]

    ds2 = xr.open_zarr(filename2,
                       decode_timedelta=True,
                       chunks='auto')
    ds2 = ds2.rename({'prediction_timedelta': 'lead_time',
                     '2m_temperature': 'tmp2m', 'total_precipitation_6hr': 'precip'})
    if 'latitude' in ds2.dims:
        ds2 = ds2.rename({'latitude': 'lat', 'longitude': 'lon'})
    ds2 = ds2[[variable]]

    # concat them together
    ds = xr.concat([ds1, ds2], dim='time')
    ds = ds.sortby('time')

    # Select only the midnight initialization times
    ds = ds.where(ds.time.dt.hour == init_hour, drop=True)

    # Round the lats to two decimal places
    ds['lat'] = np.round(ds.lat, decimals=2)
    ds['lon'] = np.round(ds.lon, decimals=2)

    # Convert units
    K_const = 273.15
    if variable == 'tmp2m':
        ds[variable] = ds[variable] - K_const
        ds.attrs.update(units='C')
        ds = ds.resample(lead_time='1D').mean(dim='lead_time')
    elif variable == 'precip':
        ds[variable] = ds[variable] * 1000.0
        ds.attrs.update(units='mm')
        # Convert from 6hrly to daily precip, robust to different numbers of 6hrly samples in a day
        ds = ds.resample(lead_time='1D').mean(dim='lead_time') * 4.0
        # Can't have precip less than zero (there are some very small negative values)
        ds = np.maximum(ds, 0)
    else:
        raise ValueError(f"Variable {variable} not implemented.")

    # Shift the lead back 6 hours + init time to align with the start date convention
    ds['lead_time'] = ds['lead_time'] - np.timedelta64(6+init_hour, 'h')

    # Convert lat/lon
    ds = lon_base_change(ds)

    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'init_hour', 'grid'],
           timeseries='time',
           chunking={"lat": 121, "lon": 240, "lead_time": 1, "time": 1000},
           cache_disable_if={'grid': 'global0_25'},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'lead_time': 1, 'time': 30}
               },
           },
           validate_cache_timeseries=False)
def graphcast_daily_regrid(start_time, end_time, variable, init_hour=0, grid='global0_25'):  # noqa: ARG001
    """Regrid for the original Weathernext datasource."""
    ds = graphcast_daily(start_time, end_time, variable, init_hour=init_hour, grid='global0_25')
    if grid == 'global0_25':
        return ds

    # Regrid onto appropriate grid
    ds = regrid(ds, grid, base='base180', method='conservative', output_chunks={"lat": 121, "lon": 240})

    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'agg_days', 'grid'],
           timeseries='time',
           cache_disable_if={'agg_days': 1},
           chunking={"lat": 121, "lon": 240, "lead_time": 10, "time": 100},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'lead_time': 1, 'time': 30}
               },
           },
           validate_cache_timeseries=False)
def graphcast_wb_rolled(start_time, end_time, variable, agg_days, grid='global0_25'):
    """A rolled and aggregated Graphcast forecast."""
    # Grab the init 0 forecast; don't need to regrid
    ds = graphcast_daily_wb(start_time, end_time, variable, init_hour=0, grid=grid)
    ds = roll_and_agg(ds, agg=agg_days, agg_col="lead_time", agg_fn="mean")
    return ds


def _process_lead(variable, lead):
    """Helper function for interpreting lead for Graphcast forecasts."""
    if variable not in ['precip', 'tmp2m']:
        raise NotImplementedError(f"Variable {variable} not implemented for Graphcast forecasts.")
    lead_params = {}
    for i in range(10):
        lead_params[f"day{i+1}"] = i
    lead_params["week1"] = 0
    lead_offset_days = lead_params.get(lead, None)
    if lead_offset_days is None:
        raise NotImplementedError(f"Lead {lead} not implemented for Graphcast {variable} forecasts.")
    agg_days = lead_to_agg_days(lead)
    return agg_days, lead_offset_days


@forecast
@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def graphcast(start_time, end_time, variable, lead, prob_type='deterministic',
              grid='global1_5', mask='lsm', region="global"):
    """Final Graphcast interface."""
    if prob_type != 'deterministic':
        raise NotImplementedError("Only deterministic forecast implemented for graphcast")

    # Process the lead
    agg_days, lead_offset_days = _process_lead(variable, lead)

    # Convert start and end time to forecast start and end based on lead time
    forecast_start = target_date_to_forecast_date(start_time, lead)
    forecast_end = target_date_to_forecast_date(end_time, lead)

    # Get the data with the right days
    ds = graphcast_wb_rolled(forecast_start, forecast_end, variable, agg_days=agg_days, grid=grid)
    ds = ds.assign_attrs(prob_type="deterministic")

    # Get specific lead
    lead_shift = np.timedelta64(lead_offset_days, 'D')
    ds = ds.sel(lead_time=lead_shift)

    # Time shift - we want target date, instead of forecast date
    ds = shift_forecast_date_to_target_date(ds, 'time', lead)

    # Round the lats/lons to two decimal places to fix the precision issue
    # Can remove these once the caches are updates
    ds['lat'] = np.round(ds.lat, decimals=2)
    ds['lon'] = np.round(ds.lon, decimals=2)
    # Apply masking and clip to region
    ds = apply_mask(ds, mask, grid=grid)
    ds = clip_region(ds, region=region)

    return ds
