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

from sheerwater_benchmarking.utils import (
    apply_mask,
    cacheable,
    clip_region,
    dask_remote,
    regrid,
    shift_forecast_date_to_target_date,
    target_date_to_forecast_date,
)


@dask_remote
@cacheable(
    data_type="array",
    cache_args=["run_type", "grid"],
    timeseries=["start_date", "model_issuance_date"],
    chunking={
        "lat": 300,
        "lon": 300,
        "lead_time": 46,
        "start_date": 29,
        "start_year": 29,
        "model_issuance_date": 1,
    },
)
def gefs_raw(start_time, end_time, run_type="perturbed", grid="global0_25"):
    """Raw GEFS forecast data.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        run_type (str): The type of run to fetch. One of:
            - perturbed: to download all perturbed runs
            - [int 0-50]: to download a specific  perturbed run
        grid (str): The grid resolution to fetch the data at. One of:
            - global0_25: 0.25 degree global grid
    """
    if grid != "global0_25":
        raise NotImplementedError("Only global 0.25 degree grid is implemented.")

    # Open the datastore from dynamical.org
    store = "https://data.dynamical.org/noaa/gefs/forecast/latest.zarr?email=info@rhizaresearch.org"

    ds = xr.open_dataset(
        store,
        engine="zarr",
        chunks={},
        drop_variables=[
            "categorical_freezing_rain_surface",
            "categorical_freezing_rain_surface_avg",
            "categorical_ice_pellets_surface",
            "categorical_ice_pellets_surface_avg",
            "categorical_rain_surface",
            "categorical_rain_surface_avg",
            "categorical_snow_surface",
            "categorical_snow_surface_avg",
            "geopotential_height_cloud_ceiling",
            "percent_frozen_precipitation_surface",
            "precipitable_water_atmosphere",
            "precipitable_water_atmosphere_avg",
            "pressure_reduced_to_mean_sea_level",
            "pressure_reduced_to_mean_sea_level_avg",
            "pressure_surface",
            "pressure_surface_avg",
            "relative_humidity_2m",
            "relative_humidity_2m_avg",
            "total_cloud_cover_atmosphere",
            "total_cloud_cover_atmosphere_avg",
            "wind_u_100m",
            "wind_u_10m",
            "wind_u_10m_avg",
            "wind_v_100m",
            "wind_v_10m",
            "wind_v_10m_avg",
            "precipitation_surface_avg",
            "temperature_2m_avg",
            "downward_short_wave_radiation_flux_surface_avg",
            "downward_long_wave_radiation_flux_surface_avg",
            "downward_short_wave_radiation_flux_surface",
            "downward_long_wave_radiation_flux_surface",
        ],
    )

    # NOTE: variables with the _avg suffix are the avg of the ensemble members

    # Select the right time period
    ds = ds.sel(init_time=slice(start_time, end_time))

    # Rename to match interface conventions
    ds = ds.rename(
        {
            "init_time": "start_date",
            "latitude": "lat",
            "longitude": "lon",
            "ensemble_member": "member",
            "precipitation_surface": "precip",
            "temperature_2m": "tmp2m",
        }
    )

    # Handle different run types
    if isinstance(run_type, int):
        # Take specific ensemble member
        ds = ds.isel(member=run_type)
    elif run_type != "perturbed":
        raise ValueError(f"Invalid run_type: {run_type}")
    # else select all ensemble members for perturbed run

    # convert units
    # precip: kg/m2 (hourly) -> mm/day requires no scalar conversion, just daily summation
    ds["precip"] = ds["precip"].resample(lead_time="D").sum()
    ds["precip"].attrs.update(units="mm/day")

    # tmp2m: just needs to be averaged over the day
    ds["tmp2m"] = ds["tmp2m"].resample(lead_time="D").mean()
    ds["tmp2m"].attrs.update(units="C")

    # Chunks need to be the same size for caching to work
    ds = ds.unify_chunks()

    return ds


@dask_remote
@cacheable(
    data_type="array",
    cache_args=["run_type", "grid"],
    timeseries=["start_date", "model_issuance_date"],
    chunking={
        "lat": 300,
        "lon": 300,
        "lead_time": 1,
        "start_date": 1000,
        "model_issuance_date": 1000,
        "start_year": 1,
        "member": 1,
    },
)
def gefs_gridded(start_time, end_time, run_type="perturbed", grid="global1_5"):
    """GEFS forecast data with regridding."""
    ds = gefs_raw(start_time, end_time, run_type, grid="global0_25")

    # Spatial resolution:
    #   0-240 hours: 0.25 degrees (~20km)
    #   243-840 hours: 0.5 degrees (~40km)
    # We need to regrid to the appropriate grid even if we are already on
    # the 0.25 degree grid because of the different resolutions at different lead times
    ds = regrid(ds, grid, base="base180", method="conservative")
    return ds


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
    lead_params = {
        "week1": 0,
        "week2": 7,
        "week3": 14,
        "week4": 21,
        "week5": 28,
        "weeks12": 0,
        "weeks23": 7,
        "weeks34": 14,
    }
    lead_offset_days = lead_params.get(lead, None)
    if lead_offset_days is None:
        raise NotImplementedError(f"Lead {lead} not implemented for GEFS forecasts.")

    # Convert start and end time to forecast start and end based on lead time
    forecast_start = target_date_to_forecast_date(start_time, lead)
    forecast_end = target_date_to_forecast_date(end_time, lead)

    if prob_type == "probabilistic":
        run_type = "perturbed"
    else:  # deterministic
        raise NotImplementedError(f"prob_type: {prob_type} not implemented for GEFS forecasts.")

    ds = gefs_gridded(
        forecast_start,
        forecast_end,
        run_type=run_type,
        grid=grid,
    )
    ds = ds.assign_attrs(prob_type=prob_type)

    # Get specific lead
    lead_shift = np.timedelta64(lead_offset_days, "D")
    ds = ds.sel(lead_time=lead_shift)

    # Time shift - we want target date, instead of forecast date
    ds = shift_forecast_date_to_target_date(ds, "start_date", lead)
    ds = ds.rename({"start_date": "time"})

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)

    # Clip to specified region
    ds = clip_region(ds, region=region)

    return ds
