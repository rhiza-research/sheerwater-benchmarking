"""Library of statistics implementations for verification."""
# flake8: noqa: D102
import xarray as xr
import numpy as np
from functools import wraps
from typing import Callable

import xskillscore
from weatherbench2.metrics import SpatialQuantileCRPS, SpatialSEEPS

from sheerwater_benchmarking.utils import cacheable

# Global metric registry dictionary
SHEERWATER_STATISTIC_REGISTRY = {}


def statistic(cache: bool = True, **cacheable_overrides):
    """A unifed statistics decorator. 

    With this decorator, you can define a statistic function that will be cached and used to compute the statistic
    for a given forecast and observation. The statistic function will be provided with the forecast, observation, and
    data as arguments, and should return a xarray.DataArray.

    The statistic function will be cached using the cacheable decorator.

    """
    def create_statistic(func):
        # Register the statistic function with the registry
        # We'll register the wrapped function instead of the original

        # Define the cacheable options for all statistic functions
        cacheable_options = {
            'data_type': 'array',
            'timeseries': 'time',
            'cache_args': [
                'variable', 'lead', 'forecast', 'truth',
                'bin_str', 'grid', 'statistic',
            ],
            'chunking': {"lat": 121, "lon": 240, "time": 1000},
            'chunk_by_arg': {'grid': {'global0_25': {"lat": 721, "lon": 1440, 'time': 30}}},
        }
        cacheable_options.update(cacheable_overrides)

        @cacheable(cache=cache, **cacheable_options)
        def global_statistic(
            start_time, end_time,
            fcst, obs, data,
            variable, lead, forecast, truth,
            bin_str, grid, statistic,
            **cache_kwargs
        ):
            # Pass the cache kwargs through
            cache_kwargs = {
                'variable': variable, 'lead': lead, 'forecast': forecast, 'truth': truth,
                'bin_str': bin_str, 'grid': grid,
                'statistic': statistic, 'start_time': start_time, 'end_time': end_time,
            }
            ds = func(fcst=fcst, obs=obs, data=data, **cache_kwargs)
            # Assign attributes in one call
            ds = ds.assign_attrs(
                sparse=data['sparse'],
                prob_type=data['prob_type'],
                forecast=forecast,
                truth=truth,
                statistic=statistic
            )
            return ds

        @wraps(func)
        def wrapper(
            fcst, obs, data, **cache_kwargs
        ):
            # Remove start and end time from the kwargs so they can be
            # passed positionally as the cacheable operator requires
            start_time = cache_kwargs.pop('start_time')
            end_time = cache_kwargs.pop('end_time')

            # Set the statistic to the function name in lowercase
            cache_kwargs['statistic'] = func.__name__.lower()

            # Call the global statistic function
            ds = global_statistic(
                start_time, end_time,
                fcst=fcst, obs=obs, data=data,
                **cache_kwargs,
            )
            return ds

        # Register the wrapped function with the registry
        SHEERWATER_STATISTIC_REGISTRY[func.__name__] = wrapper

        return wrapper
    return create_statistic


@statistic(cache=False)
def obs_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    return obs


@statistic(cache=False)
def fcst_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    return fcst


@statistic(cache=False)
def squared_obs_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    return obs**2


@statistic(cache=False)
def squared_fcst_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    return fcst**2


@statistic(cache=False)
def covariance_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    return fcst * obs


@statistic(cache=False)
def obs_anom_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    return obs - data['climatology']


@statistic(cache=False)
def fcst_anom_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    return fcst - data['climatology']


@statistic(cache=False)
def squared_obs_anom_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    obs_anom = obs_anom_fn(fcst, obs, data, **cache_kwargs)
    return obs_anom**2


@statistic(cache=False)
def squared_fcst_anom_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    fcst_anom = fcst_anom_fn(fcst, obs, data, **cache_kwargs)
    return fcst_anom**2


@statistic(cache=False)
def anom_covariance_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    fcst_anom = fcst_anom_fn(fcst, obs, data, **cache_kwargs)
    obs_anom = obs_anom_fn(fcst, obs, data, **cache_kwargs)
    return obs_anom * fcst_anom


@statistic(cache=False)
def n_valid_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    return xr.ones_like(fcst).where(fcst.notnull(), 0.0, drop=False).astype(float)


@statistic(cache=True)
def obs_digitized_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    # `np.digitize(x, bins, right=True)` returns index `i` such that:
    #   `bins[i-1] < x <= bins[i]`
    # Indices range from 0 (for x <= bins[0]) to len(bins) (for x > bins[-1]).
    # `bins` for np.digitize will be `thresholds_np`.
    ds = xr.apply_ufunc(
        np.digitize,
        obs,
        kwargs={'bins': data['bins'], 'right': True},
        dask='parallelized',
        output_dtypes=[int],
    )
    # Restore NaN values
    return ds.where(obs.notnull(), np.nan, drop=False).astype(float)


@statistic(cache=True)
def fcst_digitized_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    # `np.digitize(x, bins, right=True)` returns index `i` such that:
    #   `bins[i-1] < x <= bins[i]`
    # Indices range from 0 (for x <= bins[0]) to len(bins) (for x > bins[-1]).
    # `bins` for np.digitize will be `thresholds_np`.
    ds = xr.apply_ufunc(
        np.digitize,
        fcst,
        kwargs={'bins': data['bins'], 'right': True},
        dask='parallelized',
        output_dtypes=[int],
    )
    # Restore NaN values
    return ds.where(fcst.notnull(), np.nan)


@statistic(cache=False)
def false_positives_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    # "Positive events" are 2 (above threshold); negative are 1
    obs_dig = obs_digitized_fn(fcst, obs, data, **cache_kwargs)
    fcst_dig = fcst_digitized_fn(fcst, obs, data, **cache_kwargs)
    return (obs_dig == 1) & (fcst_dig == 2)


@statistic(cache=False)
def false_negatives_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    obs_dig = obs_digitized_fn(fcst, obs, data, **cache_kwargs)
    fcst_dig = fcst_digitized_fn(fcst, obs, data, **cache_kwargs)
    return (obs_dig == 2) & (fcst_dig == 1)


@statistic(cache=False)
def true_positives_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    obs_dig = obs_digitized_fn(fcst, obs, data, **cache_kwargs)
    fcst_dig = fcst_digitized_fn(fcst, obs, data, **cache_kwargs)
    return (obs_dig == 2) & (fcst_dig == 2)


@statistic(cache=False)
def true_negatives_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    obs_dig = obs_digitized_fn(fcst, obs, data, **cache_kwargs)
    fcst_dig = fcst_digitized_fn(fcst, obs, data, **cache_kwargs)
    return (obs_dig == 1) & (fcst_dig == 1)


@statistic(cache=False)
def n_correct_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    obs_dig = obs_digitized_fn(fcst, obs, data, **cache_kwargs)
    fcst_dig = fcst_digitized_fn(fcst, obs, data, **cache_kwargs)
    return (obs_dig == fcst_dig)


@statistic(cache=False)
def n_obs_bin_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    category = data['category']
    obs_dig = obs_digitized_fn(fcst, obs, data, **cache_kwargs)
    return (obs_dig == category)


@statistic(cache=False)
def n_fcst_bin_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    category = data['category']
    fcst_dig = fcst_digitized_fn(fcst, obs, data, **cache_kwargs)
    return (fcst_dig == category)


@statistic(cache=False)
def mae_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    return abs(fcst - obs)


@statistic(cache=False)
def mse_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    return (fcst - obs)**2


@statistic(cache=False)
def bias_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    return fcst - obs


@statistic(cache=False)
def mape_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    return abs(fcst - obs) / np.maximum(abs(obs), 1e-10)


@statistic(cache=False)
def smape_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    return abs(fcst - obs) / (abs(fcst) + abs(obs))


@statistic(cache=False)
def brier_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    fcst_dig = fcst_digitized_fn(fcst, obs, data, **cache_kwargs)
    obs_dig = obs_digitized_fn(fcst, obs, data, **cache_kwargs)
    positive_event_id = 2
    fcst_event_prob = (fcst_dig == positive_event_id).mean(dim='member')
    obs_event_prob = (obs_dig == positive_event_id)
    return (fcst_event_prob - obs_event_prob)**2


@statistic(cache=False)
def seeps_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    wet_threshold = data['wet_threshold']
    dry_fraction = data['dry_fraction']
    clim_ds = xr.merge([wet_threshold, dry_fraction])
    m_ds = SpatialSEEPS(climatology=clim_ds,
                        dry_threshold_mm=0.25,
                        precip_name='precip',
                        min_p1=0.03,
                        max_p1=0.93) \
        .compute(forecast=fcst, truth=obs,
                 avg_time=False, skipna=True)
    m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    return m_ds


@statistic(cache=False)
def crps_fn(fcst, obs, data, **cache_kwargs):  # noqa: F821
    if data['prob_type'] == 'ensemble':
        fcst = fcst.chunk(member=-1, time=1, lead_time=1, lat=250, lon=250)  # member must be -1 to succeed
        m_ds = xskillscore.crps_ensemble(observations=obs, forecasts=fcst, mean=False, dim=['time', 'lead_time'])
    elif data['prob_type'] == 'quantile':
        m_ds = SpatialQuantileCRPS(quantile_dim='member').compute(forecast=fcst, truth=obs, avg_time=False, skipna=True)
        m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    else:
        raise ValueError(f"Invalid probability type: {data['prob_type']}")
    return m_ds


def statistic_factory(statistic_name: str):
    """Get a statistic function by name from the registry."""
    return SHEERWATER_STATISTIC_REGISTRY[f'{statistic_name}_fn']


def list_statistics():
    """List all available statistics in the registry."""
    return list(SHEERWATER_STATISTIC_REGISTRY.keys())
