"""GEFS forecast data product.

Historical weather data from the Global Ensemble Forecast System (GEFS) model operated by NOAA NCEP,
transformed into zarr format by dynamical.org.

GEFS Forecast Dataset: https://dynamical.org/catalog/noaa-gefs-forecast/

Zarr Dataset: https://data.dynamical.org/noaa/gefs/forecast/latest.zarr
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
    init_time:       400
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
        units:                  seconds
    ingested_forecast_length:
        statistics_approximate: {'max': '35 days 00:00:00', 'min': '0 days 00:00:00'}
        units:                  seconds
    init_time:
        calendar:               proleptic_gregorian
        statistics_approximate: {'max': 'Present', 'min': '2024-01-01T00:00:00'}
        units:                  seconds since 1970-01-01
    latitude:
        statistics_approximate: {'max': 90.0, 'min': -90.0}
        units:                  degrees_north
    lead_time:
        statistics_approximate: {'max': '35 days 00:00:00', 'min': '0 days 00:00:00'}
        units:                  seconds
    longitude:
        statistics_approximate: {'max': 179.75, 'min': -180.0}
        units:                  degrees_east
    spatial_ref:
        crs_wkt:                     GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY[
                                     "EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","89
                                     01"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NO
                                     RTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]
        geographic_crs_name:         WGS 84
        grid_mapping_name:           latitude_longitude
        horizontal_datum_name:       World Geodetic System 1984
        inverse_flattening:          298.257223563
        longitude_of_prime_meridian: 0.0
        prime_meridian_name:         Greenwich
        reference_ellipsoid_name:    WGS 84
        semi_major_axis:             6378137.0
        semi_minor_axis:             6356752.314245179
        spatial_ref:                 GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY[
                                     "EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","89
                                     01"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NO
                                     RTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]
    valid_time:
        calendar:               proleptic_gregorian
        statistics_approximate: {'max': 'Present + 35 days', 'min': '2024-01-01T00:00:00'}
        units:                  seconds since 1970-01-01

Data Variables:
    categorical_freezing_rain_surface:
        type:       float32
        shape:      init_time * ensemble_member * lead_time * latitude * longitude
        long_name:  Categorical freezing rain
        short_name: cfrzr
        step_type:  avg
        units:      0=no; 1=yes
    categorical_freezing_rain_surface_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          Categorical freezing rain
        short_name:         cfrzr
        step_type:          avg
        units:              0=no; 1=yes
    categorical_ice_pellets_surface:
        type:       float32
        shape:      init_time * ensemble_member * lead_time * latitude * longitude
        long_name:  Categorical ice pellets
        short_name: cicep
        step_type:  avg
        units:      0=no; 1=yes
    categorical_ice_pellets_surface_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          Categorical ice pellets
        short_name:         cicep
        step_type:          avg
        units:              0=no; 1=yes
    categorical_rain_surface:
        type:       float32
        shape:      init_time * ensemble_member * lead_time * latitude * longitude
        long_name:  Categorical rain
        short_name: crain
        step_type:  avg
        units:      0=no; 1=yes
    categorical_rain_surface_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          Categorical rain
        short_name:         crain
        step_type:          avg
        units:              0=no; 1=yes
    categorical_snow_surface:
        type:       float32
        shape:      init_time * ensemble_member * lead_time * latitude * longitude
        long_name:  Categorical snow
        short_name: csnow
        step_type:  avg
        units:      0=no; 1=yes
    categorical_snow_surface_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          Categorical snow
        short_name:         csnow
        step_type:          avg
        units:              0=no; 1=yes
    downward_long_wave_radiation_flux_surface:
        type:       float32
        shape:      init_time * ensemble_member * lead_time * latitude * longitude
        long_name:  Surface downward long-wave radiation flux
        short_name: sdlwrf
        step_type:  avg
        units:      W/(m^2)
    downward_long_wave_radiation_flux_surface_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          Surface downward long-wave radiation flux
        short_name:         sdlwrf
        step_type:          avg
        units:              W/(m^2)
    downward_short_wave_radiation_flux_surface:
        type:       float32
        shape:      init_time * ensemble_member * lead_time * latitude * longitude
        long_name:  Surface downward short-wave radiation flux
        short_name: sdswrf
        step_type:  avg
        units:      W/(m^2)
    downward_short_wave_radiation_flux_surface_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          Surface downward short-wave radiation flux
        short_name:         sdswrf
        step_type:          avg
        units:              W/(m^2)
    geopotential_height_cloud_ceiling:
        type:          float32
        shape:         init_time * ensemble_member * lead_time * latitude * longitude
        long_name:     Geopotential height
        short_name:    gh
        standard_name: geopotential_height
        step_type:     instant
        units:         gpm
    percent_frozen_precipitation_surface:
        type:       float32
        shape:      init_time * ensemble_member * lead_time * latitude * longitude
        long_name:  Percent frozen precipitation
        short_name: cpofp
        step_type:  instant
        units:      %
    precipitable_water_atmosphere:
        type:       float32
        shape:      init_time * ensemble_member * lead_time * latitude * longitude
        long_name:  Precipitable water
        short_name: pwat
        step_type:  instant
        units:      kg/(m^2)
    precipitable_water_atmosphere_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          Precipitable water
        short_name:         pwat
        step_type:          instant
        units:              kg/(m^2)
    precipitation_surface:
        type:       float32
        shape:      init_time * ensemble_member * lead_time * latitude * longitude
        long_name:  Total Precipitation
        short_name: tp
        step_type:  accum
        units:      kg/(m^2)
    precipitation_surface_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          Total Precipitation
        short_name:         tp
        step_type:          accum
        units:              kg/(m^2)
    pressure_reduced_to_mean_sea_level:
        type:       float32
        shape:      init_time * ensemble_member * lead_time * latitude * longitude
        long_name:  Pressure reduced to MSL
        short_name: prmsl
        step_type:  instant
        units:      Pa
    pressure_reduced_to_mean_sea_level_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          Pressure reduced to MSL
        short_name:         prmsl
        step_type:          instant
        units:              Pa
    pressure_surface:
        type:          float32
        shape:         init_time * ensemble_member * lead_time * latitude * longitude
        long_name:     Surface pressure
        short_name:    sp
        standard_name: surface_air_pressure
        step_type:     instant
        units:         Pa
    pressure_surface_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          Surface pressure
        short_name:         sp
        standard_name:      surface_air_pressure
        step_type:          instant
        units:              Pa
    relative_humidity_2m:
        type:          float32
        shape:         init_time * ensemble_member * lead_time * latitude * longitude
        long_name:     2 metre relative humidity
        short_name:    r2
        standard_name: relative_humidity
        step_type:     instant
        units:         %
    relative_humidity_2m_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          2 metre relative humidity
        short_name:         r2
        standard_name:      relative_humidity
        step_type:          instant
        units:              %
    temperature_2m:
        type:          float32
        shape:         init_time * ensemble_member * lead_time * latitude * longitude
        long_name:     2 metre temperature
        short_name:    t2m
        standard_name: air_temperature
        step_type:     instant
        units:         C
    temperature_2m_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          2 metre temperature
        short_name:         t2m
        standard_name:      air_temperature
        step_type:          instant
        units:              C
    total_cloud_cover_atmosphere:
        type:       float32
        shape:      init_time * ensemble_member * lead_time * latitude * longitude
        long_name:  Total Cloud Cover
        short_name: tcc
        step_type:  avg
        units:      %
    total_cloud_cover_atmosphere_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          Total Cloud Cover
        short_name:         tcc
        step_type:          avg
        units:              %
    wind_u_100m:
        type:          float32
        shape:         init_time * ensemble_member * lead_time * latitude * longitude
        long_name:     100 metre U wind component
        short_name:    u100
        standard_name: eastward_wind
        step_type:     instant
        units:         m/s
    wind_u_10m:
        type:          float32
        shape:         init_time * ensemble_member * lead_time * latitude * longitude
        long_name:     10 metre U wind component
        short_name:    u10
        standard_name: eastward_wind
        step_type:     instant
        units:         m/s
    wind_u_10m_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          10 metre U wind component
        short_name:         u10
        standard_name:      eastward_wind
        step_type:          instant
        units:              m/s
    wind_v_100m:
        type:          float32
        shape:         init_time * ensemble_member * lead_time * latitude * longitude
        long_name:     100 metre V wind component
        short_name:    v100
        standard_name: northward_wind
        step_type:     instant
        units:         m/s
    wind_v_10m:
        type:          float32
        shape:         init_time * ensemble_member * lead_time * latitude * longitude
        long_name:     10 metre V wind component
        short_name:    v10
        standard_name: northward_wind
        step_type:     instant
        units:         m/s
    wind_v_10m_avg:
        type:               float32
        shape:              init_time * lead_time * latitude * longitude
        ensemble_statistic: avg
        long_name:          10 metre V wind component
        short_name:         v10
        standard_name:      northward_wind
        step_type:          instant
        units:              m/s
"""
import numpy as np
import xarray as xr
import zarr
import asyncio
from dateutil import parser

from sheerwater_benchmarking.utils import (
    apply_mask,
    cacheable,
    clip_region,
    dask_remote,
    regrid,
    shift_forecast_date_to_target_date,
    target_date_to_forecast_date,
    lead_to_agg_days,
)


@dask_remote
@cacheable(
    data_type="array",
    cache_args=["year"],
    chunking={
        "lat": 374,
        "lon": 368,
        "lead_time": 35,
        "time": 1,
        "member": 31,
    },
)
def gefs_year(year):
    """Raw GEFS forecast data.

    Args:
        year
    """
    # Open the datastore from dynamical.org
    class RetryingFsspecStore(zarr.storage.FsspecStore):
        max_attempts = 5
        backoff = 0.1

        async def get(
            self,
            key: str,
            prototype: zarr.core.buffer.core.BufferPrototype,
            byte_range: zarr.abc.store.ByteRequest | None = None,
        ) -> zarr.core.buffer.Buffer | None:
            for attempt in range(self.max_attempts):
                try:
                    return await super().get(key, prototype, byte_range)
                except:
                    if attempt >= self.max_attempts - 1:
                        raise
                    await asyncio.sleep(self.backoff * (2**attempt))

            raise AssertionError("Unreachable")

    # Usage example
    store = RetryingFsspecStore.from_url("https://data.dynamical.org/noaa/gefs/forecast-35-day/latest.zarr?email=info@rhizaresearch.org", read_only=True)
    ds = xr.open_zarr(store,
                      chunks={'latitude': 374, 'longitude': 368, 'init_time': 1, 'ensemble_member': 31, 'lead_time': 64},
                      decode_timedelta=True,
                    )

    ds = ds.sel(init_time=slice(f'{year}-01-01', f'{year}-12-31'))
    ds = ds[["precipitation_surface", "temperature_2m"]]

    # Rename to match interface conventions
    ds = ds.rename(
        {
            "init_time": "time",
            "latitude": "lat",
            "longitude": "lon",
            "ensemble_member": "member",
            "precipitation_surface": "precip",
            "temperature_2m": "tmp2m",
        }
    )

    # convert units
    # precip: kg/m2 (hourly) -> mm/day requires no scalar conversion, just daily summation
    ds["precip"] = ds["precip"].resample(lead_time="D").sum()
    ds["precip"].attrs.update(units="mm/day")

    # tmp2m: just needs to be averaged over the day
    ds["tmp2m"] = ds["tmp2m"].resample(lead_time="D").mean()
    ds["tmp2m"].attrs.update(units="C")

    return ds



@dask_remote
@cacheable(
    data_type="array",
    cache_args=[],
    timeseries=["time"],
    chunking={
        "lat": 374,
        "lon": 368,
        "lead_time": 32,
        "time": 1,
        "member": 31,
    },
)
def gefs_raw(start_time, end_time):
    """Raw GEFS forecast data.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
    """
    years = range(parser.parse(start_time).year, parser.parse(end_time).year + 1)

    datasets = []
    for year in years:
        ds = gefs_year(year, filepath_only=True)
        datasets.append(ds)

    ds = xr.open_mfdataset(datasets,
                           engine='zarr',
                           parallel=True,
                           chunks={'latitude': 374, 'longitude': 368, 'init_time': 1, 'ensemble_member': 31, 'lead_time': 96},
                           decode_timedelta=True)

    return ds


@dask_remote
@cacheable(
    data_type="array",
    cache_args=["grid"],
    timeseries=["time"],
    cache_disable_if={'grid':'global0_25'},
    chunking={
        "lat": 374,
        "lon": 368,
        "lead_time": 16,
        "time": 1,
        "member": 31,
    },
)
def gefs_gridded(start_time, end_time, grid="global0_25"):
    """GEFS forecast data with regridding."""
    ds = gefs_raw(start_time, end_time)

    # Spatial resolution:
    #   0-240 hours: 0.25 degrees (~20km)
    #   243-840 hours: 0.5 degrees (~40km)
    # We need to regrid to the appropriate grid even if we are already on
    # the 0.25 degree grid because of the different resolutions at different lead times
    ds = regrid(ds, grid, base="base180", method="conservative")
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'agg_days', 'prob_type', 'grid'],
           timeseries='time',
           cache_disable_if={'agg_days': 1},
            chunking={
                "lat": 300,
                "lon": 300,
                "lead_time": 1,
                "time": 1000,
                "member": 1,
            },
           validate_cache_timeseries=False)
def gefs_rolled(start_time, end_time, variable, agg_days, prob_type, grid='global0_25'):
    """A rolled and aggregated gefs forecast."""
    ds = gefs_gridded(start_time, end_time, grid=grid)

    if prob_type == 'probabilistic':
        ds = ds[[variable]]
        ds = ds.assign_attrs(prob_type='ensemble')
    else:
        ds = ds[[variable]]
        ds = ds.mean(dim='member')
        ds = ds.assign_attrs(prob_type='deterministic')

    ds = roll_and_agg(ds, agg=agg_days, agg_col="lead_time", agg_fn="mean")
    return ds

def _process_lead(variable, lead):
    """Helper function for interpreting lead for FuXI forecasts."""
    lead_params = {}
    for i in range(35):
        lead_params[f"day{i+1}"] = i
    for i in [0, 7, 14, 21, 28, 35]:
        lead_params[f"week{i//7+1}"] = i
    for i in [0, 7, 14, 21, 28]:
        lead_params[f"weeks{(i//7)+1}{(i//7)+2}"] = i
    lead_offset_days = lead_params.get(lead, None)
    if lead_offset_days is None:
        raise NotImplementedError(f"Lead {lead} not implemented for FuXi {variable} forecasts.")

    agg_days = lead_to_agg_days(lead)
    return agg_days, lead_offset_days


@dask_remote
@cacheable(
    data_type="array",
    timeseries="time",
    cache=False,
    cache_args=["variable", "lead", "prob_type", "grid", "mask", "region"],
)
def gefs(
    start_time,
    end_time,
    variable,
    lead,
    prob_type="probabilistic",
    grid="global1_5",
    mask="lsm",
    region="global",
):
    """Standard format forecast data for GEFS forecasts.

    Args:
        start_time: Start time for the forecast period
        end_time: End time for the forecast period
        variable: Variable to forecast (tmp2m, precip, etc.)
        lead: Forecast lead time (week1, week2, etc.)
        prob_type: Type of probability forecast (deterministic or probabilistic)
        grid: Grid resolution (global0_25, global1_5)
        mask: Mask to apply (lsm, None)
        region: Region to clip to (global, africa, conus)
    """
    agg_days, lead_offset_days = _process_lead(variable, lead)

    # Convert start and end time to forecast start and end based on lead time
    forecast_start = target_date_to_forecast_date(start_time, lead)
    forecast_end = target_date_to_forecast_date(end_time, lead)

    ds = gefs_rolled(
        forecast_start,
        forecast_end,
        variable=variable,
        agg_days=agg_days,
        prob_type=prob_type,
        grid=grid
    )

    # Get specific lead
    lead_shift = np.timedelta64(lead_offset_days, "D")
    ds = ds.sel(lead_time=lead_shift)

    # Time shift - we want target date, instead of forecast date
    ds = shift_forecast_date_to_target_date(ds, "time", lead)

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)

    # Clip to specified region
    ds = clip_region(ds, region=region)

    # Assign final prob label
    prob_label = prob_type if prob_type == 'deterministic' else 'ensemble'
    ds = ds.assign_attrs(prob_type=prob_label)

    return ds
