"""The Suitable Planting Window (SPW) tasks for the benchmarking platform."""
import xarray as xr

from functools import partial

from sheerwater_benchmarking.utils import cacheable, dask_remote, groupby_time, roll_and_agg
from sheerwater_benchmarking.metrics import get_datasource_fn

from sheerwater_benchmarking.forecasts.ecmwf_er import (
    ifs_extended_range, ifs_extended_range_debiased_regrid,
)

from sheerwater_benchmarking.utils import (apply_mask, clip_region,
                                           assign_grouping_coordinates, convert_group_to_time)

from .tasks import first_satisfied_date, average_time, convert_to_datetime


def rainy_onset_condition(da, prob_dim='member', prob_threshold=0.6):
    """Condition for the rainy season onset.

    Requires the input data to have 11d and 8d precipitation aggregations.

    If probability dimension is not present, return a boolean array.

    If a probability dimension is present, and prob_threshold is None,
    return the probability of the condition being met.

    If a probability dimension is present, and prob_threshold is not None,
    return a boolean array based on the probability threshold.

    Args:
        da (xr.Dataset): Dataset to apply the condition to.
        prob_dim (str): Name of the ensemble dimension.
        prob_threshold (float): Threshold for the probability dimension.
    """
    # Ensure the data has the required aggregations
    if 'precip_11d' not in da.data_vars or 'precip_8d' not in da.data_vars:
        raise ValueError("Data must have 11d and 8d precipitation aggregations.")

    # Check aggregation against the thresholds in mm
    cond = (da['precip_11d'] > 40.) & (da['precip_8d'] > 30.0)
    if prob_dim in da.dims:
        # If the probability dimension is present
        cond = cond.mean(dim=prob_dim)
        if prob_threshold is not None:
            # Convert to a boolean based on the probability threshold
            cond = cond > prob_threshold
    return cond


def first_rainy_onset(data, time_dim='time', time_offset=None, prob_dim='member', prob_threshold=0.6):
    """Get the rainy season onset from a forecast by looking over a rolling lead time.

    If deterministic or a specific probability threshold is passed, return the first date that the condition is met.
    Otherwise, return the probability of the condition for all dates.

    Args:
        data (xr.Dataset): Dataset to apply the condition to.
        time_dim (str): Name of the time dimension.
        time_offset (str): Base dimension to use if time is a timedelta objects (optional).
            Typical for lead-time based forecasts.
        prob_dim (str): Name of the ensemble dimension. If None, assume a deterministic forecast.
        prob_threshold (float): Threshold for the probability dimension.
    """
    # Add 8 day and 11 day rolling windows, requiring at least 50% of the days to be non-NaN
    missing_thresh = 0.5
    agg_days = 8
    agg_thresh = max(int(agg_days*missing_thresh), 1)
    data['precip_8d'] = roll_and_agg(data['precip'], agg=agg_days, agg_col=time_dim,
                                     agg_fn='sum', agg_thresh=agg_thresh)
    agg_days = 11
    agg_thresh = max(int(agg_days*missing_thresh), 1)
    data['precip_11d'] = roll_and_agg(data['precip'], agg=agg_days, agg_col=time_dim,
                                      agg_fn='sum', agg_thresh=agg_thresh)

    # Compute the rainy season onset date for deterministic or thresholded probability forecasts
    if prob_threshold is not None or prob_dim is None:
        fcst = first_satisfied_date(data, rainy_onset_condition, time_dim=time_dim,
                                    base_time=time_offset,
                                    prob_dim=prob_dim, prob_threshold=prob_threshold)
    else:
        if time_offset is not None:
            # Time column is preserved in the output, so no need to pass time_offset
            raise ValueError("Cannot pass time offset for a probabilistic forecast with no probability threshold.")
        fcst = rainy_onset_condition(data, prob_dim=prob_dim, prob_threshold=None)
    return fcst


@dask_remote
@cacheable(data_type='array',
           cache_args=['truth', 'groupby', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "time": 30}
               },
           })
def rainy_season_onset_truth(start_time, end_time,
                             truth='era5',
                             groupby=[['quarter', 'year']],
                             grid='global0_25', mask='lsm', region='global'):
    """Get the rainy season onset from a given truth source.

    Args:
        start_time (str): Start date for the truth data.
        end_time (str): End date for the truth data.
        truth (str): Name of the truth source.
        groupby (list): List of grouping categories for the time dimension.
            See `groupby_time` for more details on supported categories and format.
            To get the rainy season onset:
                per year, pass `groupby=[['year']]`.
                per quarter per year, pass `groupby=[['quarter', 'year']]`.
                per ea_rainy_season per year, pass `groupby=[['ea_rainy_season', 'year']]`.
                per ea_rainy_season, averaged over years,
                    pass `groupby=[['ea_rainy_season', 'year'], ['ea_rainy_season']]`.
                per year, averaged over years, pass `groupby=[['year'], [None]]]`.
        grid (str): Name of the grid.
        mask (str): Name of the mask.
        region (str): Name of the region.
    """
    # Get the ground truth data on a daily aggregation
    truth_fn = get_datasource_fn(truth)
    if truth == 'ghcn':
        # Call GHCN with non-default mean cell aggregation
        ds = truth_fn(start_time, end_time, 'precip', agg_days=1,
                      grid=grid, mask=mask, region=region, cell_aggregation='mean')
    else:
        ds = truth_fn(start_time, end_time, 'precip', agg_days=1,
                      grid=grid, mask=mask, region=region)

    #  First, get the rainy season onset within the first grouping value over dimension time
    agg_fn = [partial(first_rainy_onset, time_dim='time', prob_dim=None, prob_threshold=None)]

    # Returns a dataframe with a dimension 'time' corresponding to the first grouping value
    # and value 'rainy_onset' corresponding to the rainy season onset date
    if len(groupby) > 1:
        # For each additional grouping after the first, average over day of year within a grouping
        agg_fn += [partial(average_time, avg_over='time')]*(len(groupby)-1)
        # Add final conversion to datetime with no grouping
        agg_fn += [convert_to_datetime]
        groupby += [None]

    #  Apply the aggregation functions to the dataset using the groupby utility
    #  Set return_timeseries to True to get a dataset with a time dimension for each grouping
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
           })
def rainy_season_onset_forecast(start_time, end_time,
                                forecast, prob_type='probabilistic',
                                prob_threshold=None,
                                grid='global0_25', mask='lsm', region='global'):
    """Get the rainy reason onset from a given forecast.

    Args:
        start_time (str): Start date for the forecast data.
        end_time (str): End date for the forecast data.
        forecast (str): Name of the forecast source.
        prob_type (str): Type of probabilistic forecast (either 'probabilistic' or 'deterministic').
        prob_threshold (float): Threshold for the probability dimension.
            If None, returns a probability of event occurrence per start date and lead.
            If a threshold is passed, returns a specific date forecast.
        grid (str): Name of the grid.
        mask (str): Name of the mask.
        region (str): Name of the region.
    """
    # Get the forecast data in non-standard, daily, leads format from ECMWF
    run_type = 'average' if prob_type == 'deterministic' else 'perturbed'
    if forecast == "ecmwf_ifs_er":
        ds = ifs_extended_range(start_time, end_time, 'precip', forecast_type='forecast',
                                run_type=run_type, time_group='daily', grid=grid)
    elif forecast == "ecmwf_ifs_er_debiased":
        ds = ifs_extended_range_debiased_regrid(start_time, end_time, 'precip',
                                                run_type=run_type, time_group='daily', grid=grid)
    else:
        raise ValueError("Only ECMWF IFS Extended Range and debiased forecasts are supported.")

    # ECMWF doesn't support mask and region, so we need to apply them manually
    # TODO: build out this interface more formally
    ds = apply_mask(ds, mask, var='precip', grid=grid)
    ds = clip_region(ds, region=region)

    # Chain together the aggregation functions needed to compute the rainy season onset per grouping
    if prob_type == 'deterministic':
        agg_fn = partial(first_rainy_onset, time_dim='lead_time', time_offset='start_date')
    else:
        # If a non-NaN forecast is passed, produce a specific date forecast
        agg_fn = partial(first_rainy_onset, time_dim='lead_time', time_offset='start_date',
                         prob_dim='member', prob_threshold=prob_threshold)
    # Apply the aggregation function over lead time along the start_date dimension
    rainy_da = groupby_time(ds,
                            groupby=None,
                            agg_fn=agg_fn,
                            time_dim='start_date',
                            return_timeseries=True)

    rainy_ds = rainy_da.to_dataset(name='rainy_forecast')
    # TODO: why is chunking not working?
    rainy_ds = rainy_ds.chunk(-1)
    # TODO: Boolean grouping doesn't maintain the mask and region, so we need to apply them manually
    rainy_ds = apply_mask(rainy_ds, mask, var='rainy_forecast', grid=grid)
    rainy_ds = clip_region(rainy_ds, region=region)
    return rainy_ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['truth', 'forecast', 'prob_type', 'grid', 'mask', 'region'],
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
                             prob_threshold=0.60,
                             grid='global0_25', mask='lsm', region='global'):
    """Compute the error between the rainy season onset from a given truth and forecast."""
    truth_ds = rainy_season_onset_truth(
        start_time, end_time, truth=truth, groupby=groupby, grid=grid, mask=mask, region=region)
    forecast_ds = rainy_season_onset_forecast(
        start_time, end_time, forecast, prob_type, prob_threshold=prob_threshold,
        grid=grid, mask=mask, region=region)

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
