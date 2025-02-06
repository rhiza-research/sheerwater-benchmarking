"""The Suitable Planting Window (SPW) task utilities."""
from functools import partial

from sheerwater_benchmarking.utils import groupby_time
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


def spw_rainy_onset(ds, onset_group=None, aggregate_group=None, time_dim='time',
                    prob_type='ensemble', prob_dim=None, prob_threshold=None):
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
        return rainy_da

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
    return rainy_da
