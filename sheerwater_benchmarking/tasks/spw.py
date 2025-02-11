"""The Suitable Planting Window (SPW) task utilities."""
from functools import partial
import xarray as xr

from sheerwater_benchmarking.utils import groupby_time, apply_mask, clip_region, first_satisfied_date, doy_mean


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
    if 'agg_precip_11d_shift0d' not in da.data_vars or 'agg_precip_8d_shift0d' not in da.data_vars:
        raise ValueError("Data must have 11d and 8d precipitation aggregations.")
    if prob_type in ['ensemble', 'quantile'] and prob_dim not in da.dims:
        raise ValueError("Probability dimension must be present for probabilistic forecasts.")

    # Check aggregation against the thresholds in mm
    cond = (da['agg_precip_11d_shift0d'] > 40.) & (da['agg_precip_8d_shift0d'] > 30.0)

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


def rainy_onset_no_drought_condition(da, prob_type='ensemble', prob_dim='member', prob_threshold=0.6):
    """Condition for the rainy season onset with a combined no drought condition.

    Requires the input data to have 11d and 8d precipitation aggregations
    named 'precip_11d', 'precip_8d', and 'precip_11d_shift11d' respectively.

    See `rainy_onset_condition` for more details.
    """
    # Ensure the data has the required aggregations
    if 'agg_precip_11d_shift0d' not in da.data_vars or 'agg_precip_8d_shift0d' not in da.data_vars \
            or 'agg_precip_11d_shift11d' not in da.data_vars:
        raise ValueError("Data must have 8d, 11d and 11d shifted 11d precipitation aggregations.")
    if prob_type in ['ensemble', 'quantile'] and prob_dim not in da.dims:
        raise ValueError("Probability dimension must be present for probabilistic forecasts.")

    # Check aggregation against the thresholds in mm
    cond = (da['agg_precip_11d_shift0d'] > 40.) & (
        da['agg_precip_8d_shift0d'] > 30.0) & (da['agg_precip_11d_shift11d'] > 15.)

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


def spw_precip_preprocess(agg_fn, shift_fn=None,
                          agg_days=None, shift_days=None,
                          mask='lsm', region='global', grid='global1_5', ):
    """Preprocess the daily data to ensure it has the required 8 and 11 day aggregations.

    Args:
        agg_fn (callable): Function that generates a n-day aggregated precipitation dataset when
            called with the 'agg_days' keyword argument.
        shift_fn (callable): Function that generates a n-day shifted precipitation dataset when
            called with the 'shift_days' keyword argument.
        agg_days (list): List of aggregation days to compute.
        shift_days (list): List of shift days to compute.
        mask (str): Mask to apply to the data.
        region (str): Region to clip the data to.
        grid (str): Grid to regrid the data to.
    """
    if agg_days is None:
        agg_days = [8, 11]
    if shift_days is None:
        shift_days = [0] * len(agg_days)
    # Default shift function to shift by the shift_days
    if shift_fn is None:
        def shift_fn(x, shift_by_days): return x.shift(time=-shift_by_days)

    # Ensure the data has the required aggregations
    try:
        datasets = [shift_fn(
            ad * agg_fn(agg_days=ad),
            shift_by_days=sd)
            .rename({'precip': f'agg_precip_{ad}d_shift{sd}d'})
            for ad, sd in zip(agg_days, shift_days)]
    except Exception as e:
        print("Aggregating and shifting failed. Check that you have defined a proper shifting function.")
        raise e

    # Remove lead_time dimension, which may not match across datasets
    if 'lead_time' in datasets[0]:
        datasets = [ds.drop('lead_time') for ds in datasets]

    # Merge both datasets
    ds = xr.merge(datasets)

    # Apply masking
    ds = apply_mask(ds, mask, grid=grid)
    ds = clip_region(ds, region=region)
    return ds


def spw_rainy_onset(ds, onset_group=None, aggregate_group=None, time_dim='time',
                    prob_type='ensemble', prob_dim=None, prob_threshold=None,
                    drought_condition=False,
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

    if drought_condition:
        condition = rainy_onset_no_drought_condition
    else:
        condition = rainy_onset_condition

    # If no grouping is provided, compute the condition probability on the timeseries
    if prob_threshold is None and prob_type in ['ensemble', 'quantile']:
        if onset_group is not None or aggregate_group is not None:
            raise ValueError("Grouping is not supported for probabilistic forecasts without a threshold.")
        # Compute condition probability on the timeseries
        rainy_da = condition(ds, prob_type=prob_type, prob_dim=prob_dim, prob_threshold=None)
    else:
        # Otherwise, compute the first satisfied date for each grouping
        agg_fn = [partial(first_satisfied_date,
                          condition=condition,
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
    var_name = 'rainy_onset' if not drought_condition else 'rainy_onset_no_drought'
    rainy_ds = rainy_da.to_dataset(name=var_name)
    rainy_ds = apply_mask(rainy_ds, mask, grid=grid)
    rainy_ds = clip_region(rainy_ds, region=region)
    return rainy_ds
