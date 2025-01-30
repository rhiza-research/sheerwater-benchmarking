"""GEFS forecast data product.

Historical weather data from the Global Ensemble Forecast System (GEFS) model operated by NOAA NCEP,
transformed into zarr format by dynamical.org.

GEFS Forecast Dataset

Zarr Dataset: s3://us-west-2.opendata.source.coop/dynamical/noaa-gefs-forecast/v0.1.0.zarr
Attributes:
    attribution:         NOAA NWS NCEP GEFS data processed by dynamical.org from NOAA Open Data Dissemination archives.
    dataset_id:          noaa-gefs-forecast
    description:         Weather forecasts from the Global Ensemble Forecast System (GEFS) operated by NOAA NWS NCEP.
    forecast_domain:     Forecast lead time 0-840 hours (0-35 days) ahead
    forecast_resolution: Forecast step 0-240 hours: 3 hourly, 243-840 hours: 6 hourly
    name:                NOAA GEFS forecast
    spatial_domain:      Global
    spatial_resolution:  0-240 hours: 0.25 degrees (~20km), 243-840 hours: 0.5 degrees (~40km)
    time_domain:         Forecasts initialized 2024-01-01 00:00:00 UTC to Present
    time_resolution:     Forecasts initialized every 24 hours.

Dimensions:
    init_time:       390
    ensemble_member: 31
    lead_time:       181
    latitude:        721
    longitude:       1440

Coordinates:
    ensemble_member:
        statistics_approximate: {'max': 30, 'min': 0}
        units:                  realization
    expected_forecast_length:
        statistics_approximate: {'max': '35 days 00:00:00', 'min': '0 days 00:00:00'}
    ingested_forecast_length:
        statistics_approximate: {'max': '35 days 00:00:00', 'min': '0 days 00:00:00'}
    init_time:
        statistics_approximate: {'max': 'Present', 'min': '2024-01-01T00:00:00'}
    latitude:
        statistics_approximate: {'max': 90.0, 'min': -90.0}
        units:                  degrees_north
    lead_time:
        statistics_approximate: {'max': '35 days 00:00:00', 'min': '0 days 00:00:00'}
    longitude:
        statistics_approximate: {'max': 179.75, 'min': -180.0}
        units:                  degrees_south
    valid_time:
        statistics_approximate: {'max': 'Present + 35 days', 'min': '2024-01-01T00:00:00'}

Data Variables:
    categorical_freezing_rain_surface:
        type:       float32
        shape:      init_time x ensemble_member x lead_time x latitude x longitude
        long_name:  Categorical freezing rain
        short_name: cfrzr
        step_type:  avg
        units:      0=no; 1=yes
    categorical_freezing_rain_surface_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          Categorical freezing rain
        short_name:         cfrzr
        step_type:          avg
        units:              0=no; 1=yes
    categorical_ice_pellets_surface:
        type:       float32
        shape:      init_time x ensemble_member x lead_time x latitude x longitude
        long_name:  Categorical ice pellets
        short_name: cicep
        step_type:  avg
        units:      0=no; 1=yes
    categorical_ice_pellets_surface_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          Categorical ice pellets
        short_name:         cicep
        step_type:          avg
        units:              0=no; 1=yes
    categorical_rain_surface:
        type:       float32
        shape:      init_time x ensemble_member x lead_time x latitude x longitude
        long_name:  Categorical rain
        short_name: crain
        step_type:  avg
        units:      0=no; 1=yes
    categorical_rain_surface_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          Categorical rain
        short_name:         crain
        step_type:          avg
        units:              0=no; 1=yes
    categorical_snow_surface:
        type:       float32
        shape:      init_time x ensemble_member x lead_time x latitude x longitude
        long_name:  Categorical snow
        short_name: csnow
        step_type:  avg
        units:      0=no; 1=yes
    categorical_snow_surface_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          Categorical snow
        short_name:         csnow
        step_type:          avg
        units:              0=no; 1=yes
    downward_long_wave_radiation_flux_surface:
        type:       float32
        shape:      init_time x ensemble_member x lead_time x latitude x longitude
        long_name:  Surface downward long-wave radiation flux
        short_name: sdlwrf
        step_type:  avg
        units:      W/(m^2)
    downward_long_wave_radiation_flux_surface_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          Surface downward long-wave radiation flux
        short_name:         sdlwrf
        step_type:          avg
        units:              W/(m^2)
    downward_short_wave_radiation_flux_surface:
        type:       float32
        shape:      init_time x ensemble_member x lead_time x latitude x longitude
        long_name:  Surface downward short-wave radiation flux
        short_name: sdswrf
        step_type:  avg
        units:      W/(m^2)
    downward_short_wave_radiation_flux_surface_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          Surface downward short-wave radiation flux
        short_name:         sdswrf
        step_type:          avg
        units:              W/(m^2)
    geopotential_height_cloud_ceiling:
        type:          float32
        shape:         init_time x ensemble_member x lead_time x latitude x longitude
        long_name:     Geopotential height
        short_name:    gh
        standard_name: geopotential_height
        step_type:     instant
        units:         gpm
    percent_frozen_precipitation_surface:
        type:       float32
        shape:      init_time x ensemble_member x lead_time x latitude x longitude
        long_name:  Percent frozen precipitation
        short_name: cpofp
        step_type:  instant
        units:      %
    precipitable_water_atmosphere:
        type:       float32
        shape:      init_time x ensemble_member x lead_time x latitude x longitude
        long_name:  Precipitable water
        short_name: pwat
        step_type:  instant
        units:      kg/(m^2)
    precipitable_water_atmosphere_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          Precipitable water
        short_name:         pwat
        step_type:          instant
        units:              kg/(m^2)
    precipitation_surface:
        type:       float32
        shape:      init_time x ensemble_member x lead_time x latitude x longitude
        long_name:  Total Precipitation
        short_name: tp
        step_type:  accum
        units:      kg/(m^2)
    precipitation_surface_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          Total Precipitation
        short_name:         tp
        step_type:          accum
        units:              kg/(m^2)
    pressure_reduced_to_mean_sea_level:
        type:       float32
        shape:      init_time x ensemble_member x lead_time x latitude x longitude
        long_name:  Pressure reduced to MSL
        short_name: prmsl
        step_type:  instant
        units:      Pa
    pressure_reduced_to_mean_sea_level_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          Pressure reduced to MSL
        short_name:         prmsl
        step_type:          instant
        units:              Pa
    pressure_surface:
        type:          float32
        shape:         init_time x ensemble_member x lead_time x latitude x longitude
        long_name:     Surface pressure
        short_name:    sp
        standard_name: surface_air_pressure
        step_type:     instant
        units:         Pa
    pressure_surface_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          Surface pressure
        short_name:         sp
        standard_name:      surface_air_pressure
        step_type:          instant
        units:              Pa
    relative_humidity_2m:
        type:          float32
        shape:         init_time x ensemble_member x lead_time x latitude x longitude
        long_name:     2 metre relative humidity
        short_name:    r2
        standard_name: relative_humidity
        step_type:     instant
        units:         %
    relative_humidity_2m_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          2 metre relative humidity
        short_name:         r2
        standard_name:      relative_humidity
        step_type:          instant
        units:              %
    temperature_2m:
        type:          float32
        shape:         init_time x ensemble_member x lead_time x latitude x longitude
        long_name:     2 metre temperature
        short_name:    t2m
        standard_name: air_temperature
        step_type:     instant
        units:         C
    temperature_2m_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          2 metre temperature
        short_name:         t2m
        standard_name:      air_temperature
        step_type:          instant
        units:              C
    total_cloud_cover_atmosphere:
        type:       float32
        shape:      init_time x ensemble_member x lead_time x latitude x longitude
        long_name:  Total Cloud Cover
        short_name: tcc
        step_type:  avg
        units:      %
    total_cloud_cover_atmosphere_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          Total Cloud Cover
        short_name:         tcc
        step_type:          avg
        units:              %
    wind_u_100m:
        type:          float32
        shape:         init_time x ensemble_member x lead_time x latitude x longitude
        long_name:     100 metre U wind component
        short_name:    u100
        standard_name: eastward_wind
        step_type:     instant
        units:         m/s
    wind_u_10m:
        type:          float32
        shape:         init_time x ensemble_member x lead_time x latitude x longitude
        long_name:     10 metre U wind component
        short_name:    u10
        standard_name: eastward_wind
        step_type:     instant
        units:         m/s
    wind_u_10m_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          10 metre U wind component
        short_name:         u10
        standard_name:      eastward_wind
        step_type:          instant
        units:              m/s
    wind_v_100m:
        type:          float32
        shape:         init_time x ensemble_member x lead_time x latitude x longitude
        long_name:     100 metre V wind component
        short_name:    v100
        standard_name: northward_wind
        step_type:     instant
        units:         m/s
    wind_v_10m:
        type:          float32
        shape:         init_time x ensemble_member x lead_time x latitude x longitude
        long_name:     10 metre V wind component
        short_name:    v10
        standard_name: northward_wind
        step_type:     instant
        units:         m/s
    wind_v_10m_avg:
        type:               float32
        shape:              init_time x lead_time x latitude x longitude
        ensemble_statistic: avg
        long_name:          10 metre V wind component
        short_name:         v10
        standard_name:      northward_wind
        step_type:          instant
        units:              m/s
"""
import numpy as np
import xarray as xr

from sheerwater_benchmarking.utils import (
    apply_mask,
    cacheable,
    clip_region,
    dask_remote,
    lon_base_change,
    regrid,
    shift_forecast_date_to_target_date,
    target_date_to_forecast_date,
)


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'forecast_type',
                       'run_type', 'time_group', 'grid'],
           cache=False,
           timeseries=['start_date', 'model_issuance_date'],
           chunking={"lat": 300, "lon": 300, "lead_time": 46,
                     "start_date": 29, "start_year": 29,
                     "model_issuance_date": 1})
def gefs_raw(start_time, end_time, variable, forecast_type,
            run_type='average', time_group='weekly', grid="global0_25"):
    """Raw GEFS forecast data.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "reforecast".
        run_type (str): The type of run to fetch. One of:
            - average: to download the averaged of the perturbed runs
            - perturbed: to download all perturbed runs
            - [int 0-50]: to download a specific  perturbed run
        time_group (str): The time grouping to use. One of: "daily", "weekly", "biweekly"
        grid (str): The grid resolution to fetch the data at. One of:
            - global0_25: 0.25 degree global grid
    """
    if grid != 'global0_25':
        raise NotImplementedError(
            "Only global 0.25 degree grid is implemented.")

    # Open the datastore from dynamical.org
    store = 's3://us-west-2.opendata.source.coop/dynamical/noaa-gefs-forecast/v0.1.0.zarr'

    ds = xr.open_dataset(
        store,
        engine='zarr',
        backend_kwargs={
            'storage_options': {'anon': True},
            'consolidated': True
        }
    )

    # Select the right time period
    ds = ds.sel(init_time=slice(start_time, end_time))

    # Rename to match interface conventions
    ds = ds.rename(
        {
            'init_time': 'start_date',
            'latitude': 'lat',
            'longitude': 'lon',
            'ensemble_member': 'member'
        }
    )

    # Handle different run types
    if run_type == 'average':
        # Take ensemble mean
        ds = ds.mean(dim='member')
    elif isinstance(run_type, int):
        # Take specific ensemble member
        ds = ds.isel(member=run_type)
    elif run_type != 'perturbed':
        raise ValueError(f"Invalid run_type: {run_type}")

    # TODO: Select the right variable?
    # Should that be done here caching each variable separately
    # or in the gefs function caching all variables together?

    # TODO: is forecast_type a relevant argument for this dataset?

    # TODO: is time_group a relevant argument for this dataset?

    # TODO: figure out what field conversions/aggregations need to be done
    # TODO: figure out what fields can be dropped
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'forecast_type',
                       'run_type', 'time_group', 'grid'],
           cache=False,
           timeseries=['start_date', 'model_issuance_date'],
           chunking={"lat": 300, "lon": 300, "lead_time": 1,
                     "start_date": 1000,
                     "model_issuance_date": 1000, "start_year": 1,
                     "member": 1})
def gefs_gridded(start_time, end_time, variable, forecast_type,
                run_type='average', time_group='weekly', grid="global1_5"):
    """GEFS forecast data with regridding."""
    ds = gefs_raw(start_time, end_time, variable, forecast_type,
                 run_type, time_group=time_group, grid='global0_25')

    # TODO: verify if this is necessary
    # Convert to base180 longitude
    ds = lon_base_change(ds, to_base="base180")

    if grid == 'global0_25':
        return ds

    # Regrid onto appropriate grid
    ds = regrid(ds, grid, base='base180', method='conservative')
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def gefs(start_time, end_time, variable, lead, prob_type='deterministic',
        grid='global1_5', mask='lsm', region="global"):
    """Standard format forecast data for GEFS forecasts.

    Args:
        start_time: Start time for the forecast period
        end_time: End time for the forecast period
        variable: Variable to forecast (tmp2m, precip, etc.)
        lead: Forecast lead time (week1, week2, etc.)
        prob_type: Type of probability forecast (deterministic or ensemble)
        grid: Grid resolution (global0_25, global1_5)
        mask: Mask to apply (lsm, None)
        region: Region to clip to (global, africa, conus)
    """
    lead_params = {
        "week1": ('weekly', 0),
        "week2": ('weekly', 7),
        "week3": ('weekly', 14),
        "week4": ('weekly', 21),
        "weeks12": ('biweekly', 0),
        "weeks23": ('biweekly', 7),
        "weeks34": ('biweekly', 14),
    }
    time_group, lead_offset_days = lead_params.get(lead, (None, None))
    if time_group is None:
        raise NotImplementedError(
            f"Lead {lead} not implemented for GEFS forecasts.")

    # Convert start and end time to forecast start and end based on lead time
    forecast_start = target_date_to_forecast_date(start_time, lead)
    forecast_end = target_date_to_forecast_date(end_time, lead)

    if prob_type == 'deterministic':
        run_type = 'average'
    else: # probabilistic
        run_type = 'perturbed'

    ds = gefs_gridded(
        forecast_start,
        forecast_end,
        variable,
        forecast_type="forecast",
        run_type=run_type,
        time_group=time_group,
        grid=grid,
    )
    ds = ds.assign_attrs(prob_type=prob_type)

    # Get specific lead
    lead_shift = np.timedelta64(lead_offset_days, 'D')
    ds = ds.sel(lead_time=lead_shift)

    # Time shift - we want target date, instead of forecast date
    ds = shift_forecast_date_to_target_date(ds, 'start_date', lead)
    ds = ds.rename({'start_date': 'time'})

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)

    # Clip to specified region
    ds = clip_region(ds, region=region)

    return ds
