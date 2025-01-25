"""Module that contains the task API for the benchmarking platform."""
import numpy as np
import xarray as xr

from functools import partial

from sheerwater_benchmarking.utils import cacheable, dask_remote, groupby_time, start_remote, roll_and_agg
from sheerwater_benchmarking.utils.time_utils import dayofyear_to_datetime
from sheerwater_benchmarking.metrics import get_datasource_fn

from sheerwater_benchmarking.forecasts.ecmwf_er import (
    ifs_extended_range, ifs_extended_range_debiased, ifs_extended_range_debiased_regrid,
)
from sheerwater_benchmarking.forecasts.salient import salient_daily_gap

from sheerwater_benchmarking.utils import (plot_ds, apply_mask, clip_region,
                                           assign_grouping_coordinates, convert_group_to_time)

# def suitable_planting_window(ds):
# """Task to get the suitable planting window for a given location."""


def rainy_onset_condition(da, prob_dim='member', prob_threshold=0.5):
    """Condition for the rainy season onset."""
    cond = (da['precip_11d'] > 40.) & (da['precip_8d'] > 30.0)
    if prob_dim in da.dims:
        # If the probability dimension is present
        cond = cond.mean(dim=prob_dim)
        if prob_threshold is not None:
            # Convert to a boolean based on the probability threshold
            cond = cond > prob_threshold
    return cond


def first_satisfied_date(ds, condition, time_dim='time', base_time=None, prob_dim='member', prob_threshold=0.5):
    """Find the first date that a condition is satisfied in a timeseries.

    If the time dimension is a timedelta object, a base time must be specified. 
    If a prob_dim is specified, find the first date that the condition is met with 
    a probability greater than prob_threshold.

    Args:
        ds (xr.Dataset): Dataset to apply the condition to.
        condition (callable): Condition to apply to the dataset.
        time_dim (str): Name of the time dimension.
        base_time (str): Base time for timedelta objects (optional).
        prob_dim (str): Name of the ensemble dimension.
        prob_threshold (float): Threshold for the probability dimension.
    """
    # Apply the rainy reason onset condition to the grouped dataframe
    ds['condition'] = condition(ds, prob_dim, prob_threshold)

    # Ensure that dates are sorted
    ds = ds.sortby(time_dim)

    # Check if the time dimension is a timedelta object
    if np.issubdtype(ds[time_dim].dtype, np.timedelta64):
        if base_time is None:
            raise ValueError("If using timedelta64, must specify a base time for the timedelta.")
        fill_value = np.timedelta64('NaT')
    else:
        if base_time is not None:
            raise ValueError("Base time should only be specified if using a timedelta64 object.")
        fill_value = np.datetime64('NaT')

    # Get the first date that the condition is met; fill with known NaN value
    first_date = ds.condition.idxmax(dim=time_dim, fill_value=fill_value)
    # If the max value is the same as the first value and the first value is 0,
    # the condition was never met

    first_date = first_date.where((first_date > ds[time_dim].values[0]) |
                                  (ds.condition.isel({time_dim: 0}) == 1), other=fill_value)

    if np.issubdtype(ds[time_dim].dtype, np.timedelta64):
        # Add timedelta to the base time
        first_date = first_date[base_time] + first_date

    # Rename the variable
    first_date = first_date.rename('first_occurrence')
    return first_date


def first_rain(data, time_dim='time', time_offset=None, prob_dim='member', prob_threshold=0.5):
    # Add the relevant rolling values and left-align the rolling windows
    dsp = data.copy()
    # Roll and agg
    missing_thresh = 0.5
    agg_days = 8
    agg_thresh = max(int(agg_days*missing_thresh), 1)
    dsp['precip_8d'] = roll_and_agg(dsp['precip'], agg=agg_days, agg_col=time_dim, agg_fn='sum', agg_thresh=agg_thresh)
    agg_days = 11
    agg_thresh = max(int(agg_days*missing_thresh), 1)
    dsp['precip_11d'] = roll_and_agg(dsp['precip'], agg=agg_days, agg_col=time_dim, agg_fn='sum', agg_thresh=agg_thresh)
    fsd = first_satisfied_date(dsp, rainy_onset_condition, time_dim=time_dim, base_time=time_offset,
                               prob_dim=prob_dim, prob_threshold=prob_threshold)
    return fsd


def rainy_season_fcst(data, time_dim='time', time_offset=None, prob_dim='member', prob_threshold=0.5):
    # Add the relevant rolling values and left-align the rolling windows
    dsp = data.copy()
    dsp['precip_8d'] = dsp['precip'].rolling({time_dim: 8}).sum()
    dsp['precip_8d'] = dsp['precip_8d'].shift({time_dim: -7})
    dsp['precip_11d'] = dsp['precip'].rolling({time_dim: 11}).sum()
    dsp['precip_11d'] = dsp['precip_11d'].shift({time_dim: -10})
    dsp = dsp.dropna(time_dim, how='all')
    if prob_threshold is not None:
        fcst = first_satisfied_date(dsp, rainy_onset_condition, time_dim=time_dim, base_time=time_offset,
                                    prob_dim=prob_dim, prob_threshold=prob_threshold)
    else:
        fcst = rainy_onset_condition(dsp, prob_dim=prob_dim, prob_threshold=prob_threshold)
    return fcst


def average_time(data, avg_over='time'):
    """For a dataset with values in a datetime format, convert to doy and average over dim."""
    dsp = data.copy()
    # Convert to dayofyear for averaging
    dsp = dsp.dt.dayofyear
    return dsp.mean(dim=avg_over, skipna=True)


def convert_to_datetime(data):
    """Convert day of year to datetime.

    TODO: For this to work, needed to compute the underlying dask array. Shouldn't have to do this.
    """
    return xr.apply_ufunc(
        dayofyear_to_datetime,  # Function to apply
        data.compute(),
        vectorize=True,  # Ensures element-wise operation
        output_dtypes=[np.datetime64]  # Specify output dtype
    )


@dask_remote
@cacheable(data_type='array',
           cache_args=['truth', 'groupby', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "time": 30}
               },
           },
           cache=True)
def rainy_season_onset_truth(start_time, end_time,
                             truth='era5',
                             groupby=[['quarter', 'year']],
                             grid='global0_25', mask='lsm', region='global'):
    """Get the rainy reason onset from a given forecast."""
    # Get the ground truth data
    truth_fn = get_datasource_fn(truth)
    ds = truth_fn(start_time, end_time, 'precip', agg_days=1,
                  grid=grid, mask=mask, region=region, cell_aggregation='mean')

    agg_fn = [partial(first_rain, time_dim='time')]
    if len(groupby) > 1:
        # For each additional grouping after the first, average over day of year
        # and convert back to datetime at the end with no grouping
        agg_fn += [partial(average_time, avg_over='time')]*(len(groupby)-1) + [convert_to_datetime]
        groupby += [None]

    # Add time groups
    rainy_da = groupby_time(ds,
                            groupby=groupby,
                            agg_fn=agg_fn,
                            time_dim='time',
                            return_timeseries=True)

    rainy_ds = rainy_da.to_dataset(name='rainy_onset')
    rainy_ds = rainy_ds.chunk({'lat': 121, 'lon': 240, 'time': 1000})
    return rainy_ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['forecast', 'prob_type', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "time": 30}
               },
           },
           cache=True)
def rainy_season_onset_forecast(start_time, end_time,
                                forecast, prob_type='probabilistic',
                                grid='global0_25', mask='lsm', region='global'):
    """Get the rainy reason onset from a given forecast."""
    run_type = 'average' if prob_type == 'deterministic' else 'perturbed'
    if forecast == "ecmwf_ifs_er":
        ds = ifs_extended_range(start_time, end_time, 'precip', forecast_type='forecast',
                                run_type=run_type, time_group='daily', grid=grid)
    elif forecast == "ecmwf_ifs_er_debiased":
        ds = ifs_extended_range_debiased_regrid(start_time, end_time, 'precip',
                                                run_type=run_type, time_group='daily', grid=grid)
    elif forecast == 'salient':
        ds = salient_daily_gap(start_time, end_time, 'precip', grid=grid)
    else:
        # Get the ground truth data
        # forecast_fn = get_datasource_fn(forecast)
        # ds = forecast_fn(start_time, end_time, 'precip', time_group='daily',
        #                  grid=grid, mask=mask, region=region)
        raise ValueError("Only ECMWF IFS Extended Range and debiased forecasts are supported.")

    # Apply masking
    ds = apply_mask(ds, mask, var='precip', grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)

    if prob_type == 'deterministic':
        agg_fn = partial(first_rain, time_dim='lead_time', time_offset='start_date')
    else:
        # agg_fn = partial(first_rain, time_dim='lead_time', time_offset='start_date',
        #                  prob_dim='member', prob_threshold=0.25)
        agg_fn = partial(rainy_season_fcst, time_dim='lead_time', time_offset='start_date',
                         prob_dim='member', prob_threshold=None)
    # Add time groups
    rainy_da = groupby_time(ds,
                            groupby=None,
                            agg_fn=agg_fn,
                            time_dim='start_date',
                            return_timeseries=True)

    rainy_ds = rainy_da.to_dataset(name='rainy_forecast')
    # TODO: why is chunking not working?
    # rainy_ds = rainy_ds.chunk({'lat': 121, 'lon': 240, 'start_date': 1000})
    rainy_ds = rainy_ds.chunk(-1)
    # Apply masking
    rainy_ds = apply_mask(rainy_ds, mask, var='rainy_forecast', grid=grid)
    # Clip to specified region
    rainy_ds = clip_region(rainy_ds, region=region)
    return rainy_ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['forecast', 'prob_type', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "time": 30}
               },
           },
           cache=False)
def rainy_season_onset_error(start_time, end_time,
                             truth, forecast,
                             groupby=[['quarter', 'year']],
                             prob_type='probabilistic',
                             grid='global0_25', mask='lsm', region='global'):
    """Get the rainy reason onset from a given forecast."""
    # if any([isinstance(x, list) for x in grouping]):
    #     raise ValueError("Only flat grouping is supported for error calculation.")

    truth_ds = rainy_season_onset_truth(
        start_time, end_time, truth=truth, groupby=groupby, grid=grid, mask=mask, region=region)
    forecast_ds = rainy_season_onset_forecast(
        start_time, end_time, forecast, prob_type, grid=grid, mask=mask, region=region)

    # Assign grouping coordinates to forecast and merge with truth
    forecast_ds = assign_grouping_coordinates(forecast_ds, groupby[0], time_dim='start_date')
    forecast_ds = forecast_ds.assign_coords(
        time=("start_date", convert_group_to_time(forecast_ds['group'], groupby[0])))
    forecast_ds = forecast_ds.drop_vars('group')
    truth_expanded = truth_ds.sel(time=forecast_ds['time'])
    ds = xr.merge([truth_expanded, forecast_ds])

    # Compute derived metrics
    # How does the model perform in terms of days of error per start date?
    ds['error'] = (ds['rainy_forecast'] - ds['rainy_onset']).dt.days
    # How does the model perform as we approach the rainy reason?
    ds['look_ahead'] = (ds['start_date'] - ds['rainy_onset']).dt.days
    # What lead (in days) was the forecast made at?
    ds['lead'] = (ds['rainy_forecast'] - ds['start_date']).dt.days

    return ds


def growing_days():
    """Task to get the suitable planting window for a given location."""
    pass


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['truth', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "time": 30}
               },
           },
           cache=True)
def rain_windowed_spw(start_time, end_time,
                      truth='era5',
                      grid='global1_5', mask='lsm', region='global'):
    """Get a rolling window of precipitation from a rainfall datasource."""
    # Get the ground truth data
    source_fn = get_datasource_fn(truth)
    try:
        ds = source_fn(start_time, end_time, 'precip', agg_days=1,
                       grid=grid, mask=mask, region=region, cell_aggregation='mean')
    except TypeError:
        # Run without GHCN specific cell aggregation flag
        ds = source_fn(start_time, end_time, 'precip', agg_days=1,
                       grid=grid, mask=mask, region=region)
    missing_thresh = 0.5
    agg_days = 8
    agg_thresh = max(int(agg_days*missing_thresh), 1)
    ds['precip_8d'] = roll_and_agg(ds['precip'], agg=agg_days, agg_col='time', agg_fn='sum', agg_thresh=agg_thresh)
    agg_days = 11
    agg_thresh = max(int(agg_days*missing_thresh), 1)
    ds['precip_11d'] = roll_and_agg(ds['precip'], agg=agg_days, agg_col='time', agg_fn='sum', agg_thresh=agg_thresh)

    ds = ds.drop_vars('spatial_ref')
    ds = ds.to_dataframe()
    return ds


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['truth', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "time": 30}
               },
           },
           cache=True)
def rainy_onset_spw(start_time, end_time,
                    truth='era5',
                    grid='global1_5', mask='lsm', region='global'):
    """Get the rainy reason onset from a given truth source, according to the SPW method."""
    ds = rainy_season_onset_truth(start_time, end_time, truth=truth,
                                  groupby=[['ea_rainy_season', 'year']],
                                  region=region, mask=mask, grid=grid)
    ds = ds.drop_vars('spatial_ref')
    df = ds.to_dataframe()
    df = df.dropna(subset='rainy_onset')
    return df


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['start_time', 'end_time', 'forecast', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "time": 30}
               },
           },
           cache=True)
def rainy_onset_spw_forecast(start_time, end_time,
                             forecast='ecmwf_ifs_er_debiased',
                             grid='global1_5', mask='lsm', region='global'):
    """Get the rainy reason onset from a given truth source, according to the SPW method."""
    # Get the forecast for rainy season onset
    ds = rainy_season_onset_forecast(start_time, end_time,
                                     forecast=forecast,
                                     region=region, grid=grid, mask=mask)
    ds = ds.drop_vars('spatial_ref')
    df = ds.to_dataframe()
    df = df.dropna(subset='rainy_forecast')
    return df


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['agg_days', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "time": 30}
               },
           },
           cache=True)
def rainfall_data(start_time, end_time, agg_days=1,
                  grid='global1_5', mask='lsm', region='global'):
    """Get rainfall values from all data sources."""
    # Get the ground truth data
    datasets = []
    for truth in ['era5', 'chirps', 'imerg', 'ghcn']:
        source_fn = get_datasource_fn(truth)
        try:
            ds = source_fn(start_time, end_time, 'precip', agg_days=agg_days,
                           grid=grid, mask=mask, region=region, cell_aggregation='mean')
        except:
            ds = source_fn(start_time, end_time, 'precip', agg_days=agg_days,
                           grid=grid, mask=mask, region=region)
        ds = ds.rename({'precip': f'{truth}_precip'})
        datasets.append(ds)

    # Merge datasets
    ds = xr.merge(datasets, join='outer')
    ds = ds.drop_vars('spatial_ref')

    # Convert to dataframe
    ds = ds.to_dataframe()
    return ds


if __name__ == '__main__':
    start_date = "2020-01-01"
    end_date = "2022-01-01"

    start_remote(remote_config='xlarge_cluster', remote_name='genevieve2')
    # ds = rainy_season_onset_forecast(start_date, end_date, forecast='ecmwf_ifs_er', prob_type='probabilistic',
    #                                  grid='global1_5', mask='lsm', region='africa')
    ds = rainy_season_onset_error(start_date, end_date, truth='era5', forecast='ecmwf_ifs_er',
                                  groupby=[['ea_rainy_season', 'year']],
                                  prob_type='probabilistic',
                                  grid='global1_5', mask='lsm', region='kenya')
