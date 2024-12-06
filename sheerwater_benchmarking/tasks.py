"""Module that contains the task API for the benchmarking platform."""
import numpy as np
import xarray as xr

from sheerwater_benchmarking.utils import cacheable, dask_remote, groupby_time, start_remote
from sheerwater_benchmarking.utils.time_utils import dayofyear_to_datetime
from sheerwater_benchmarking.metrics import get_datasource_fn


# def suitable_planting_window(ds):
# """Task to get the suitable planting window for a given location."""


def rainy_onset_condition(da):
    """Condition for the rainy season onset."""
    return (da['precip_11d'] > 40.) & (da['precip_8d'] > 30.0)


def first_satisfied_date(ds, condition, time_dim='time'):
    """Find the first date that a condition is satisfied in a timeseries."""
    # Apply the rainy reason onset condition to the grouped dataframe
    ds['condition'] = condition(ds)

    # Ensure that dates are sorted
    ds = ds.sortby(time_dim)

    # Get the first date that the condition is met; fill with known NaN value
    first_date = ds.condition.idxmax(dim=time_dim, fill_value=np.datetime64('NaT'))
    # If the max value is the same as the first value, the condition was never met
    first_date = first_date.where(first_date > ds.time.values[0], other=np.datetime64('NaT'))
    first_date = first_date.rename('first_occurrence')
    return first_date


@dask_remote
@cacheable(data_type='array',
           cache_args=['truth', 'groupby', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "time": 30}
               },
           },
           cache=False)
def rainy_season_onset_truth(start_time, end_time,
                             truth='era5',
                             groupby=['quarter', 'year'],
                             grid='global0_25', mask='lsm', region='global'):
    """Get the rainy reason onset from a given forecast."""
    # Get the ground truth data
    truth_fn = get_datasource_fn(truth)
    ds = truth_fn(start_time, end_time, 'precip', time_group='daily',
                  grid=grid, mask=mask, region=region)

    def first_rain(data):
        # Add the relevant rolling values and left-align the rolling windows
        dsp = data.copy()
        dsp['precip_8d'] = dsp['precip'].rolling(time=8).sum()
        dsp['precip_11d'] = dsp['precip'].rolling(time=11).sum()
        dsp['precip_8d'] = dsp['precip_8d'].shift(time=-7)
        dsp['precip_11d'] = dsp['precip_11d'].shift(time=-10)
        return first_satisfied_date(dsp, rainy_onset_condition, time_dim='time')

    def average_time(data):
        dsp = data.copy()
        # Convert to dayofyear for averaging
        dsp = dsp.dt.dayofyear
        return dsp.mean(dim='time', skipna=True)

    def convert_to_datetime(data):
        return xr.apply_ufunc(
            dayofyear_to_datetime,  # Function to apply
            data.compute(),
            vectorize=True,  # Ensures element-wise operation
            output_dtypes=[np.datetime64]  # Specify output dtype
        )

    agg_fn = [first_rain]
    if len(groupby) > 1:
        # For each additional grouping after the first, average over day of year
        # and convert back to datetime at the end with no grouping
        agg_fn += [average_time]*(len(groupby)-1) + [convert_to_datetime]
        groupby += [None]

    # Add time groups
    rainy_ds = groupby_time(ds,
                            groupby=groupby,
                            agg_fn=agg_fn,
                            time_dim='time',
                            return_timeseries=True)
    
    return rainy_ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['forecast', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "time": 30}
               },
           },
           cache=False)
def rainy_season_onset_forecast(start_time, end_time,
                                forecast, prob_type='deterministic',
                                grid='global0_25', mask='lsm', region='global'):
    """Get the rainy reason onset from a given forecast."""
    # Get the ground truth data
    truth_fn = get_datasource_fn(forecast)
    ds = truth_fn(start_time, end_time, 'precip', lead='daily',
                  grid=grid, mask=mask, region=region)

    # Add the relevant rolling values and left-align the rolling windows
    ds['precip_8d'] = ds['precip'].rolling(lead=8).sum()
    ds['precip_11d'] = ds['precip'].rolling(lead=11).sum()
    ds['precip_8d'] = ds['precip_8d'].shift(lead=-7)
    ds['precip_11d'] = ds['precip_11d'].shift(lead=-10)

    # Add time groups
    rainy_ds = groupby_time(ds,
                            grouping=time_grouping,
                            agg_fn=first_satisfied_date,
                            return_timeseries=True,
                            condition=rainy_onset_condition)
    return rainy_ds


def growing_days():
    """Task to get the suitable planting window for a given location."""
    pass


if __name__ == '__main__':
    start_date = "2020-01-01"
    end_date = "2022-01-01"

    start_remote(remote_config='xlarge_cluster')
    ds = rainy_season_onset(start_date, end_date, lead='day1', forecast='era5', time_grouping='quarter')
    import pdb
    pdb.set_trace()
