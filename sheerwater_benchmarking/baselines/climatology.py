"""A climatology baseline forecast for benchmarking."""
import dateparser
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
import xarray as xr
import dask
from functools import partial
from sheerwater_benchmarking.reanalysis import era5_daily, era5_rolled
from sheerwater_benchmarking.utils import (dask_remote, cacheable, get_dates,
                                           apply_mask, clip_region, pad_with_leapdays, add_dayofyear)
from sheerwater_benchmarking.tasks import spw_rainy_onset, spw_precip_preprocess


@dask_remote
@cacheable(data_type='array',
           cache_args=['first_year', 'last_year', 'agg_days', 'grid'],
           chunking={"lat": 721, "lon": 1440, "dayofyear": 30},
           auto_rechunk=False)
def seeps_dry_fraction(first_year=1985, last_year=2014, agg_days=7, grid='global1_5'):
    """Compute the climatology of the ERA5 data. Years are inclusive."""
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year}-12-31"

    # Get the rolled era5 data
    ds = era5_rolled(start_time, end_time, variable='precip', agg_days=agg_days, grid=grid)

    # Add day of year as a coordinate
    ds = add_dayofyear(ds)
    ds = pad_with_leapdays(ds)

    ds['is_dry'] = ds['precip'] < 0.25
    ds = ds.groupby('dayofyear').mean(dim='time')
    ds = ds.drop_vars(['precip'])
    ds = ds.rename({
        'is_dry': 'dry_fraction',
    })

    # Convert to true day of year
    ds['dayofyear'] = ds.dayofyear.dt.dayofyear

    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['first_year', 'last_year', 'agg_days', 'grid'],
           chunking={"lat": 721, "lon": 1440, "dayofyear": 30},
           auto_rechunk=False)
def seeps_wet_threshold(first_year=1985, last_year=2014, agg_days=7, grid='global1_5'):
    """Compute the climatology of the ERA5 data. Years are inclusive."""
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year}-12-31"

    # Get the rolled era5 data
    ds = era5_rolled(start_time, end_time, variable='precip', agg_days=agg_days, grid=grid)

    # Add day of year as a coordinate
    ds = add_dayofyear(ds)
    ds = pad_with_leapdays(ds)

    ds = ds.groupby('dayofyear').quantile(2/3, method='nearest', dim='time')

    ds = ds.rename({
        'precip': 'wet_threshold',
    })

    # Convert to true day of year
    ds['dayofyear'] = ds.dayofyear.dt.dayofyear

    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'first_year', 'last_year', 'grid'],
           chunking={"lat": 721, "lon": 1440, "dayofyear": 30},
           auto_rechunk=False)
def climatology_raw(variable, first_year=1985, last_year=2014, grid='global1_5'):
    """Compute the climatology of the ERA5 data. Years are inclusive."""
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year}-12-31"

    # Get single day, masked data between start and end years
    ds = era5_daily(start_time, end_time, variable=variable, grid=grid)

    # Add day of year as a coordinate
    ds = add_dayofyear(ds)
    ds = pad_with_leapdays(ds)

    # Take average over the period to produce climatology that includes leap years
    ds = ds.groupby('dayofyear').mean(dim='time')

    return ds


@dask_remote
@cacheable(data_type='array',
           cache=True,
           cache_args=['variable', 'first_year', 'last_year', 'prob_type', 'agg_days', 'grid'],
           chunking={"lat": 121, "lon": 240, "dayofyear": 1000, "member": 1},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'dayofyear': 30, 'member': 1}
               }
           },
           auto_rechunk=False)
def climatology_agg_raw(variable, first_year=1985, last_year=2014,
                        prob_type='deterministic', agg_days=7, grid="global1_5"):
    """Generates aggregated climatology."""
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year}-12-31"
    ds = era5_rolled(start_time, end_time, variable=variable, agg_days=agg_days, grid=grid)

    # Add day of year as a coordinate
    ds = add_dayofyear(ds)
    ds = pad_with_leapdays(ds)

    # Take average over the period to produce climatology
    if prob_type == 'deterministic':
        return ds.groupby('dayofyear').mean(dim='time')
    elif prob_type == 'probabilistic':
        # Otherwise, get ensemble members sampled from climatology
        def sample_members(sub_ds, members=30):
            doy = sub_ds.dayofyear.values[0]
            ind = np.random.randint(0, len(sub_ds.time.values), size=(members,))
            sub = sub_ds.isel(time=ind)
            sub = sub.assign_coords(time=np.arange(members)).rename({'time': 'member'})
            sub = sub.assign_coords(dayofyear=doy)
            return sub

        doys = []
        for doy in np.unique(ds.dayofyear.values):
            doys.append(
                sample_members(ds.isel(time=(ds.dayofyear.values == doy))))
        ds = xr.concat(doys, dim='dayofyear')
        ds = ds.chunk(member=1)
        return ds
    else:
        raise ValueError(f"Unsupported prob_type: {prob_type}")


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'clim_years', 'agg_days', 'grid'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               }
           },
           cache=True)
def climatology_rolling_agg(start_time, end_time, variable, clim_years=30, agg_days=7, grid="global1_5"):
    """Compute a rolling {clim_years}-yr climatology of the ERA5 data.

    Args:
        start_time: First time of the forecast period.
        end_time: Last time of the forecast period.
        variable: Variable to compute climatology for.
        clim_years: Number of years to compute climatology over.
        agg_days (int): Aggregation period in days.
        grid: Grid resolution of the data.
    """
    #  Get reanalysis data for the appropriate look back period
    # We need data from clim_years before the start_time until 1 year before the end_time
    # as this climatology excludes the most recent year for use in operational forecasting
    new_start = (dateparser.parse(start_time) - relativedelta(years=clim_years)).strftime("%Y-%m-%d")
    new_end = (dateparser.parse(end_time) - relativedelta(years=1)).strftime("%Y-%m-%d")

    # Get ERA5 data, and ignore cache validation if start_time is earlier than the cache
    ds = era5_rolled(new_start, new_end, variable=variable, agg_days=agg_days, grid=grid)
    ds = add_dayofyear(ds)
    ds = pad_with_leapdays(ds)

    def doy_rolling(sub_ds, years):
        return sub_ds.rolling(time=years, min_periods=years, center=False).mean()

    # Rechunk the data to have a single time chunk for efficient rolling
    ds = ds.chunk(time=1)
    ds = ds.groupby('dayofyear').map(doy_rolling, years=clim_years)
    ds = ds.dropna('time', how='all')
    ds = ds.drop('dayofyear')
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'agg_days', 'grid'],
           chunking={"lat": 300, "lon": 300, "time": 366})
def _era5_rolled_for_clim(start_time, end_time, variable, agg_days=7, grid="global1_5"):
    """Aggregates the hourly ERA5 data into daily data and rolls.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        agg_days (int): The aggregation period to use, in days
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
            - global0_25: 0.25 degree global grid
    """
    # Get single day, masked data between start and end years
    ds = era5_rolled(start_time, end_time, variable=variable, agg_days=agg_days, grid=grid)

    # Add day of year as a coordinate
    ds = add_dayofyear(ds)
    ds = pad_with_leapdays(ds)
    ds = ds.assign_coords(year=ds.time.dt.year)
    ds = ds.chunk({'lat': 300, 'lon': 300, 'time': 366})
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'first_year', 'last_year', 'agg_days', 'grid'],
           chunking={"lat": 121, "lon": 240, "dayofyear": 366},
           cache=True,
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'dayofyear': 30}
               }
           },
           auto_rechunk=False)
def climatology_linear_weights(variable, first_year=1985, last_year=2014, agg_days=7, grid='global1_5'):
    """Fit the climatological trend for a specific day of year.

    Args:
        variable: Variable to compute climatology for.
        first_year: First year of the climatology.
        last_year: Last year of the climatology.
        agg_days: Aggregation period in days.
        grid: Grid resolution of the data.
    """
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year}-12-31"

    # Get single day, masked data between start and end years
    ds = _era5_rolled_for_clim(start_time, end_time, variable=variable, agg_days=agg_days, grid=grid)

    def fit_trend(sub_ds):
        return sub_ds.swap_dims({"time": "year"}).polyfit(dim='year', deg=1)
    # Fit the trend for each day of the year
    ds = ds.groupby('dayofyear').map(fit_trend)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'first_year', 'last_year', 'trend', 'prob_type', 'agg_days', 'grid'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               }
           })
def climatology_rolled(start_time, end_time, variable, first_year=1985, last_year=2014,
                       trend=False, prob_type='deterministic', agg_days=7, grid="global1_5"):
    """Generates a forecast timeseries of climatology.

    Args:
        start_time (str): The start time of the timeseries forecast.
        end_time (str): The end time of the timeseries forecast.
        variable (str): The weather variable to fetch.
        first_year (int): The first year to use for the climatology.
        last_year (int): The last year to use for the climatology.
        trend (bool): Whether to include a trend in the forecast.
        prob_type (str): The type of forecast to generate.
        agg_days (int): The aggregation period to use, in days
        grid (str): The grid to produce the forecast on.
    """
    # Create a target date dataset
    target_dates = get_dates(start_time, end_time, stride='day', return_string=False)
    time_ds = xr.Dataset({'time': target_dates})
    time_ds = add_dayofyear(time_ds)

    if trend:
        if prob_type == 'probabilistic':
            raise NotImplementedError("Probabilistic trend forecasts are not supported.")

        time_ds = time_ds.assign_coords(year=time_ds['time'].dt.year)
        coeff = climatology_linear_weights(variable, first_year=first_year, last_year=last_year,
                                           agg_days=agg_days, grid=grid)
        with dask.config.set(**{'array.slicing.split_large_chunks': True}):
            coeff = coeff.sel(dayofyear=time_ds.dayofyear)
            coeff = coeff.drop('dayofyear')

        def linear_fit(coeff):
            """Compute the linear fit y = a * year + b for the given coefficients."""
            est = coeff[f"{variable}_polyfit_coefficients"].sel(degree=1) * coeff["year"].astype("float") \
                + coeff[f"{variable}_polyfit_coefficients"].sel(degree=0)
            est = est.to_dataset(name=variable)
            if variable == 'precip':
                est = np.maximum(est, 0)
            return est
        ds = linear_fit(coeff)
        ds = ds.drop('year')
    else:
        # Get climatology on the corresponding global grid
        ds = climatology_agg_raw(variable, first_year=first_year, last_year=last_year,
                                 prob_type=prob_type, agg_days=agg_days, grid=grid)
        # Select the climatology data for the target dates, and split large chunks
        with dask.config.set(**{'array.slicing.split_large_chunks': True}):
            ds = ds.sel(dayofyear=time_ds.dayofyear)
            ds = ds.drop('dayofyear')
    return ds


def _process_lead(variable, lead):
    """Helper function for interpreting lead for climatology forecasts."""
    lead_params = {}
    for i in range(1, 366):
        lead_params[f"day{i}"] = 1
    if variable != 'rainy_onset':
        for i in range(1, 7):
            lead_params[f"week{i}"] = 7
        for le in ['weeks12', 'weeks23', 'weeks34', 'weeks45', 'weeks56']:
            lead_params[le] = 14

    agg_days = lead_params.get(lead, None)
    if agg_days is None:
        raise NotImplementedError(f"Lead {lead} not implemented for climatology.")
    return agg_days


@dask_remote
def climatology_spw(start_time, end_time, first_year=1985, last_year=2014, trend=False,
                    prob_type='probabilistic', prob_threshold=0.2,
                    drought_condition=False,
                    onset_group=['ea_rainy_season', 'year'], aggregate_group=None,
                    grid='global1_5', mask='lsm', region="global"):
    """Climatology SPW forecast."""
    prob_label = prob_type if prob_type == 'deterministic' else 'ensemble'
    fn = partial(climatology_rolled, start_time, end_time, variable='precip',
                 first_year=first_year, last_year=last_year,
                 trend=trend, prob_type=prob_type, grid=grid)
    roll_days = [8, 11] if not drought_condition else [8, 11, 11]
    shift_days = [0, 0] if not drought_condition else [0, 0, 11]
    data = spw_precip_preprocess(fn, agg_days=roll_days, shift_days=shift_days,
                                 mask=mask, region=region, grid=grid)

    (prob_dim, prob_threshold) = ('member', prob_threshold) if prob_type == 'probabilistic' else (None, None)
    ds = spw_rainy_onset(data,
                         onset_group=onset_group, aggregate_group=aggregate_group,
                         time_dim='time',
                         prob_type=prob_label, prob_dim=prob_dim, prob_threshold=prob_threshold,
                         drought_condition=drought_condition,
                         mask=mask, region=region, grid=grid)
    return ds


@dask_remote
def _climatology_unified(start_time, end_time, variable, lead,
                         first_year=1985, last_year=2014, trend=False,
                         prob_type='deterministic',
                         grid='global0_25', mask='lsm', region='global'):
    """Standard format forecast data for climatology forecast."""
    agg_days = _process_lead(variable, lead)
    # Get daily data
    if variable == 'rainy_onset' or variable == 'rainy_onset_no_drought':
        drought_condition = variable == 'rainy_onset_no_drought'
        ds = climatology_spw(start_time, end_time, first_year=first_year, last_year=last_year,
                             trend=trend,
                             prob_type=prob_type, prob_threshold=0.2,
                             onset_group=['ea_rainy_season', 'year'], aggregate_group=None,
                             drought_condition=drought_condition,
                             grid=grid, mask=mask, region=region)
        # Rainy onset is sparse, so we need to set the sparse attribute
        ds = ds.assign_attrs(sparse=True)
    else:
        ds = climatology_rolled(start_time, end_time, variable,
                                first_year=first_year, last_year=last_year,
                                trend=trend,
                                prob_type=prob_type,
                                agg_days=agg_days, grid=grid)
        # Apply masking and clip to region
        ds = apply_mask(ds, mask, grid=grid)
        ds = clip_region(ds, region=region)

    if prob_type == 'deterministic':
        ds = ds.assign_attrs(prob_type="deterministic")
    else:
        ds = ds.assign_attrs(prob_type="ensemble")
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def climatology_2015(start_time, end_time, variable, lead, prob_type='deterministic',
                     grid='global0_25', mask='lsm', region='global'):
    """Standard format forecast data for climatology forecast."""
    return _climatology_unified(start_time, end_time, variable, lead, first_year=1985, last_year=2014,
                                trend=False, prob_type=prob_type, grid=grid, mask=mask, region=region)


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def climatology_2020(start_time, end_time, variable, lead, prob_type='deterministic',
                     grid='global0_25', mask='lsm', region='global'):
    """Standard format forecast data for climatology forecast."""
    return _climatology_unified(start_time, end_time, variable, lead, first_year=1990, last_year=2019,
                                trend=False, prob_type=prob_type, grid=grid, mask=mask, region=region)


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def climatology_trend_2015(start_time, end_time, variable, lead, prob_type='deterministic',
                           grid='global0_25', mask='lsm', region='global'):
    """Standard format forecast data for climatology forecast."""
    return _climatology_unified(start_time, end_time, variable, lead, first_year=1985, last_year=2014,
                                trend=True, prob_type=prob_type, grid=grid, mask=mask, region=region)


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def climatology_rolling(start_time, end_time, variable, lead, prob_type='deterministic',
                        grid='global0_25', mask='lsm', region='global'):
    """Standard format forecast data for climatology forecast."""
    lead_params = {
        "week1": 7,
        "week2": 7,
        "week3": 7,
        "week4": 7,
        "week5": 7,
        "week6": 7,
        "weeks12": 14,
        "weeks23": 14,
        "weeks34": 14,
        "weeks45": 14,
        "weeks56": 14,
    }

    agg_days = lead_params.get(lead, None)
    if agg_days is None:
        raise NotImplementedError(f"Lead {lead} not implemented for rolling climatology.")

    if prob_type != 'deterministic':
        raise NotImplementedError("Only deterministic forecasts are available for rolling climatology.")

    # Get daily data
    start_dt = dateparser.parse(start_time)
    start_dt -= relativedelta(years=1)  # exclude the most recent year for operational forecasting (handles leap year)
    new_start = datetime.strftime(start_dt, "%Y-%m-%d")

    end_dt = dateparser.parse(end_time)
    end_dt -= relativedelta(years=1)  # exclude the most recent year for operational forecasting (handles leap year)
    new_end = datetime.strftime(end_dt, "%Y-%m-%d")

    ds = climatology_rolling_agg(new_start, new_end, variable, clim_years=30, agg_days=agg_days, grid=grid)

    # Undo yearly time shifting
    times = [x + pd.DateOffset(years=1) for x in ds.time.values]
    ds = ds.assign_coords(time=times)

    # Handle duplicate values due to leap years
    # TODO: handle this in a more general way
    ds = ds.drop_duplicates(dim='time')

    ds = ds.assign_attrs(prob_type="deterministic")

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)
    return ds
