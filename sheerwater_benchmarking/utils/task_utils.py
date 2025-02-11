"""The task functions for the benchmarking platform."""
import numpy as np


def first_satisfied_date(ds, condition, time_dim='time', base_time=None,
                         prob_type='ensemble', prob_dim='member', prob_threshold=0.5):
    """Find the first date that a condition is satisfied in a timeseries.

    If the time dimension is a timedelta object, a base time must be specified.
    If a prob_dim is specified, find the first date that the condition is met with
    a probability greater than prob_threshold.

    Args:
        ds (xr.Dataset): Dataset to apply the condition to.
        condition (callable): Condition to apply to the dataset.
        time_dim (str): Name of the time dimension.
        base_time (str): Base time for timedelta objects (optional).
        prob_type (str): Type of probabilistic forecast. One of 'ensemble', 'quantile', or 'deterministic'.
        prob_dim (str): Name of the ensemble dimension.
        prob_threshold (float): Threshold for the probability dimension.
    """
    # Apply the rainy reason onset condition to the grouped dataframe
    ds['condition'] = condition(ds, prob_type=prob_type, prob_dim=prob_dim, prob_threshold=prob_threshold)

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
