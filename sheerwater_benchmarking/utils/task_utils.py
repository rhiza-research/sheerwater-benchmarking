"""The task functions for the benchmarking platform."""
import numpy as np
import xarray as xr


from .time_utils import dayofyear_to_datetime


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


def average_time(data, avg_over='time'):
    """Utility function for rainy onset.

    For a dataset with values in a datetime format, convert to doy and average over dim.
    """
    # Convert to dayofyear for averaging
    dsp = data.dt.dayofyear
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
