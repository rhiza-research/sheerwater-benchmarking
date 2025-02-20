"""Interface for gencast forecasts."""
import xarray as xr
import numpy as np
import gcsfs

from sheerwater_benchmarking.utils import (dask_remote, cacheable,
                                           apply_mask, clip_region,
                                           lon_base_change,
                                           target_date_to_forecast_date,
                                           shift_forecast_date_to_target_date,
                                           lead_to_agg_days, roll_and_agg, regrid)


@dask_remote
@cacheable(data_type='array',
           cache_args=['year', 'variable', 'init_hour'],
           chunking={"lat": 721, "lon": 1440, 'lead_time': 10, 'time': 1, 'member': 5})
def gencast_daily_year(year, variable, init_hour=0):
    """A daily Gencast forecast."""
    if init_hour != 0:
        raise ValueError("Only 0 init hour supported")

    # Get glob of all gencast forecasts
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')

    def get_sub_ds(root):
        members = fs.ls(root, detail=False)
        members = members[1:]
        zarrs = []
        for mem in members:
            mem_path = mem + '/forecasts_15d/'
            zarr = fs.ls(mem_path, detail=False)
            print(zarr)
            zarr = 'gs://' + zarr[0]
            ds = xr.open_zarr(zarr, chunks='auto', decode_timedelta=True)
            if ds.attrs['evaluation_wid'] <= 7:
                zarrs.append(zarr)
            else:
                print("Skipping overlapping samples")

        return zarrs

    zarrs = []

    if year == '2020':
        zarrs += get_sub_ds('gs://weathernext/126478713_1_0/zarr/124883614_2020_to_2021')
    elif year == '2021':
        zarrs += get_sub_ds('gs://weathernext/126478713_1_0/zarr/124958964_2021_to_2022')
    elif year == '2022':
        zarrs += get_sub_ds('gs://weathernext/126478713_1_0/zarr/125057031_2022_to_2023')
    elif year == '2023':
        zarrs += get_sub_ds('gs://weathernext/126478713_1_0/zarr/125156073_2023_to_2024')

    ds = xr.open_mfdataset(zarrs,
                           chunks='auto',
                           decode_timedelta=True,
                           engine='zarr',
                           drop_variables=['100m_u_component_of_wind',
                                           '100m_v_component_of_wind',
                                           '10m_u_component_of_wind',
                                           '10m_v_component_of_wind',
                                           'geopotential',
                                           'mean_sea_level_pressure',
                                           'sea_surface_temperature',
                                           'specific_humidity',
                                           'temperature',
                                           'u_component_of_wind',
                                           'v_component_of_wind',
                                           'vertical_velocity'
                                           ])

    # Read the three years for gcloud
    ds = ds.rename({'prediction_timedelta': 'lead_time',
                    '2m_temperature': 'tmp2m',
                    'total_precipitation_12hr': 'precip',
                    'sample': 'member'})
    ds = ds[[variable]]

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
        ds = ds.resample(lead_time='1D').mean(dim='lead_time') * 2.0

        # Can't have precip less than zero (there are some very small negative values)
        ds = np.maximum(ds, 0)
    else:
        raise ValueError(f"Variable {variable} not implemented.")

    # Shift the lead back 12 hours + init time to be aligned
    ds['lead_time'] = ds['lead_time'] - np.timedelta64(init_hour+12, 'h')

    # Convert lat/lon
    ds = lon_base_change(ds)

    ds = ds.chunk({"lat": 721, "lon": 1440, 'lead_time': 10, 'time': 1, 'member': 5})

    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid'],
           timeseries='time',
           chunking={"lat": 121, "lon": 240, "lead_time": 10, "time": 10, 'member': 10},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'lead_time': 10, 'time': 1, 'member': 5}
               },
           },
           validate_cache_timeseries=False)
def gencast_daily(start_time, end_time, variable, grid='global0_25'):  # noqa: ARG001
    """A daily gencast forecast."""
    ds1 = gencast_daily_year(year='2020', variable=variable, init_hour=0)
    ds2 = gencast_daily_year(year='2021', variable=variable, init_hour=0)
    ds3 = gencast_daily_year(year='2022', variable=variable, init_hour=0)

    ds = xr.concat([ds1, ds2, ds3], dim='time')

    ds = ds.chunk({'lat': 721, 'lon': 1440, 'lead_time': 10, 'time': 1, 'member': 5})

    # Regrid
    if grid != 'global0_25':
        ds = regrid(ds, grid, base='base180', method='conservative',
                    output_chunks={"lat": 721, "lon": 1440})

    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'agg_days', 'prob_type', 'grid'],
           timeseries='time',
           cache_disable_if={'agg_days': 1, 'prob_type': 'probabilistic'},
           chunking={"lat": 121, "lon": 240, "lead_time": 10, "time": 10, "member": 10},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'lead_time': 10, 'time': 1, 'member': 5}
               },
           },
           validate_cache_timeseries=False)
def gencast_rolled(start_time, end_time, variable, agg_days, prob_type='deterministic', grid='global0_25'):
    """A rolled and aggregated gencast forecast."""
    ds = gencast_daily(start_time, end_time, variable, grid)

    if prob_type == 'deterministic':
        ds = ds.mean(dim='member')
        ds = ds.assign_attrs(prob_type="deterministic")
    else:
        ds = ds.assign_attrs(prob_type="ensemble")

    ds = roll_and_agg(ds, agg=agg_days, agg_col="lead_time", agg_fn="mean")
    return ds


def _process_lead(variable, lead):
    """Helper function for interpreting lead for gencast forecasts."""
    if variable not in ['precip', 'tmp2m']:
        raise NotImplementedError(f"Variable {variable} not implemented for gencast forecasts.")
    lead_params = {}
    for i in range(15):
        lead_params[f"day{i+1}"] = i
    lead_params["week1"] = 0
    lead_params["week2"] = 7
    lead_offset_days = lead_params.get(lead, None)
    if lead_offset_days is None:
        raise NotImplementedError(f"Lead {lead} not implemented for gencast {variable} forecasts.")
    agg_days = lead_to_agg_days(lead)
    return agg_days, lead_offset_days


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def gencast(start_time, end_time, variable, lead, prob_type='deterministic',
            grid='global1_5', mask='lsm', region="global"):
    """Final Gencast interface."""
    if variable != 'precip':
        raise NotImplementedError("Data error present in non-precip variables in Gencast. Skipping.")

    # Process the lead
    agg_days, lead_offset_days = _process_lead(variable, lead)

    # Convert start and end time to forecast start and end based on lead time
    forecast_start = target_date_to_forecast_date(start_time, lead)
    forecast_end = target_date_to_forecast_date(end_time, lead)

    # Get the data with the right days
    ds = gencast_rolled(forecast_start, forecast_end, variable, agg_days=agg_days, prob_type=prob_type, grid=grid)

    # Get specific lead
    lead_shift = np.timedelta64(lead_offset_days, 'D')
    ds = ds.sel(lead_time=lead_shift)

    # Time shift - we want target date, instead of forecast date
    ds = shift_forecast_date_to_target_date(ds, 'time', lead)

    # Apply masking and clip to region
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    ds = clip_region(ds, region=region)

    return ds
