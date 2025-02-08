"""The Suitable Planting Window (SPW) task utilities."""
from functools import partial
import xarray as xr

from sheerwater_benchmarking.utils import groupby_time, apply_mask, clip_region
from sheerwater_benchmarking.utils import first_satisfied_date, doy_mean


def rainy_onset_condition(da, prob_type='ensemble', prob_dim='member', prob_threshold=0.6):
    """Condition for the rainy season onset.

    Requires the input data to have 11d and 8d precipitation aggregations
    named 'precip_11d' and 'precip_8d' respectively.

    If prob_type is deterministic, return a boolean array.

    If a prob_type is probabilistic (ensemble, quantile) and prob_threshold is None,
    return the probability of the condition being met.

    If prob_type is probabilistic (ensemble, quantile) and prob_threshold is not None,
    return a boolean array based on the probability threshold.

    Args:
        da (xr.Dataset): Dataset to apply the condition to.
        prob_type (str): Type of probability to use, one of 'deterministic', 'ensemble', 'quantile'
        prob_dim (str): Name of the probabilistic dimension to average over
        prob_threshold (float): Threshold for the probability dimension.
    """
    # Ensure the data has the required aggregations
    if 'precip_11d' not in da.data_vars or 'precip_8d' not in da.data_vars:
        raise ValueError("Data must have 11d and 8d precipitation aggregations.")
    if prob_type in ['ensemble', 'quantile'] and prob_dim not in da.dims:
        raise ValueError("Probability dimension must be present for probabilistic forecasts.")

    # Check aggregation against the thresholds in mm
    cond = (da['precip_11d'] > 40.) & (da['precip_8d'] > 30.0)

    if prob_type == 'ensemble':
        # If the probability dimension is present
        cond = cond.mean(dim=prob_dim)
        if prob_threshold is not None:
            # Convert to a boolean based on the probability threshold
            cond = cond > prob_threshold
    elif prob_type == 'quantile':
        # If the quantile dimension is present
        raise NotImplementedError("Quantile probability not implemented yet.")
    return cond


def spw_precip_preprocess(fn, mask='lsm', region='global', grid='global1_5'):
    """Preprocess the daily data to ensure it has the required 8 and 11 day aggregations.

    Args:
        fn (callable): Function that generates a n-day aggregated precipitation dataset when 
            called with the 'agg_days' keyword argument.
        mask (str): Mask to apply to the data.
        region (str): Region to clip the data to.
        grid (str): Grid to regrid the data to.
    """
    # Ensure the data has the required aggregations
    datasets = [agg_days*fn(agg_days=agg_days)
                .rename({'precip': f'precip_{agg_days}d'})
                for agg_days in [8, 11]]

    # Merge both datasets
    ds = xr.merge(datasets)

    # Apply masking
    ds = apply_mask(ds, mask, grid=grid)
    ds = clip_region(ds, region=region)
    return ds


def spw_rainy_onset(ds, onset_group=None, aggregate_group=None, time_dim='time',
                    prob_type='ensemble', prob_dim=None, prob_threshold=None,
                    mask='lsm', region='global', grid='global1_5'):
    """Utility function to get first rainy onset."""
    # Ensure that onset group and aggregate groups are lists
    if onset_group and not isinstance(onset_group, list):
        onset_group = [onset_group]
    if aggregate_group and not isinstance(aggregate_group, list):
        aggregate_group = [aggregate_group]

    # Form the groupby list for grouping utility
    groupby = [onset_group]
    if aggregate_group:
        groupby.append(aggregate_group)

    # If no grouping is provided, compute the condition probability on the timeseries
    if prob_threshold is None and prob_type in ['ensemble', 'quantile']:
        if onset_group is not None or aggregate_group is not None:
            raise ValueError("Grouping is not supported for probabilistic forecasts without a threshold.")
        # Compute condition probability on the timeseries
        rainy_da = rainy_onset_condition(ds, prob_type=prob_type, prob_dim=prob_dim, prob_threshold=None)
    else:
        # Otherwise, compute the first satisfied date for each grouping
        agg_fn = [partial(first_satisfied_date,
                          condition=rainy_onset_condition,
                          prob_type=prob_type, prob_dim=prob_dim, prob_threshold=prob_threshold,
                          time_dim=time_dim)]

        # Returns a dataframe with a dimension 'time' corresponding to the first grouping value
        # and value 'rainy_onset' corresponding to the rainy season onset date
        if aggregate_group is not None:
            # For the aggregate grouping average over datetimes within a grouping
            agg_fn += [partial(doy_mean, dim='time')]

        #  Apply the aggregation functions to the dataset using the groupby utility
        #  Set return_timeseries to True to get a dataset with a time dimension for each grouping
        rainy_da = groupby_time(ds, groupby=groupby, agg_fn=agg_fn,
                                time_dim=time_dim, return_timeseries=True)

    # Apply masking and clip to region
    rainy_ds = rainy_da.to_dataset(name='rainy_onset')
    rainy_ds = apply_mask(rainy_ds, mask, grid=grid)
    rainy_ds = clip_region(rainy_ds, region=region)
    return rainy_ds
