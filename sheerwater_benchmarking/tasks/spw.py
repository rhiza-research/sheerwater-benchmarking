"""The Suitable Planting Window (SPW) task utilities."""
from functools import partial

from sheerwater_benchmarking.utils import groupby_time
from sheerwater_benchmarking.utils import first_satisfied_date, average_time, convert_to_datetime


def rainy_onset_condition(da, prob_dim='member', prob_threshold=0.6, time_dim='time'):
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
        time_dim (str): Name of the time dimension.
    """
    # Ensure the data has the required aggregations
    if 'precip_11d' not in da.data_vars or 'precip_8d' not in da.data_vars:
        raise ValueError("Data must have 11d and 8d precipitation aggregations.")

    # Check aggregation against the thresholds in mm
    cond = (da['precip_11d'] > 40.) & (da['precip_8d'] > 30.0)
    if 'rainy_onset_ltn' in da.data_vars:
        cond &= (da[time_dim].dt.dayofyear >= da['rainy_onset_ltn'].dt.dayofyear)

    if prob_dim in da.dims:
        # If the probability dimension is present
        cond = cond.mean(dim=prob_dim)
        if prob_threshold is not None:
            # Convert to a boolean based on the probability threshold
            cond = cond > prob_threshold
    return cond


def spw_rainy_onset(ds, groupby, time_dim='time', prob_dim=None, prob_threshold=None):
    """Utility function to get first rainy onset."""
    # Ensure groupby is a list of lists
    if groupby and not isinstance(groupby, list):
        groupby = [groupby]
    if groupby and not isinstance(groupby[0], list):  # TODO: this is not fully general
        groupby = [groupby]

    # Compute the rainy season onset date for deterministic or thresholded probability forecasts
    if prob_threshold is None and prob_dim is not None:
        if groupby is not None:
            raise ValueError("Grouping is not supported for probabilistic forecasts without a threshold.")
        # Compute condition probability on the timeseries
        rainy_da = rainy_onset_condition(ds, prob_dim=prob_dim, prob_threshold=None, time_dim=time_dim)
        return rainy_da

    # Perform grouping
    agg_fn = [partial(first_satisfied_date,
                      condition=rainy_onset_condition,
                      time_dim=time_dim, base_time=None,
                      prob_dim=prob_dim, prob_threshold=prob_threshold)]

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
    rainy_da = groupby_time(ds, groupby=groupby, agg_fn=agg_fn,
                            time_dim=time_dim, return_timeseries=True)
    return rainy_da
