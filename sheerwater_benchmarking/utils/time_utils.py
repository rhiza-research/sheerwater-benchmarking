"""Time and date utility functions for all parts of the data pipeline."""
from calendar import isleap
import xarray as xr
import pandas as pd
import numpy as np
import dateparser
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY, MONTHLY, WEEKLY, YEARLY
from dateutil.relativedelta import relativedelta

DATETIME_FORMAT = "%Y-%m-%d"

# Lead aggregation period in days and offset
LEAD_OFFSETS = {
    'week1': (7, (0, 'days')),
    'week2': (7, (7, 'days')),
    'week3': (7, (14, 'days')),
    'week4': (7, (21, 'days')),
    'week5': (7, (28, 'days')),
    'week6': (7, (35, 'days')),
    'weeks12': (14, (0, 'days')),
    'weeks34': (14, (14, 'days')),
    'weeks56': (14, (28, 'days')),
}


def lead_to_agg_days(lead):
    """Convert lead to time grouping."""
    return LEAD_OFFSETS[lead][0]


def dayofyear_to_datetime(x):
    """Converts day of year to datetime."""
    if np.isnan(x):
        return np.datetime64('NaT', 'ns')
    return np.datetime64("1904-01-01", 'ns') + np.timedelta64(int(x), 'D')


def shift_forecast_date_to_target_date(ds, forecast_date_dim, lead):
    """Shift a forecast date dimension to a target date coordinate from a lead time."""
    ds = ds.assign_coords(forecast_date_dim=[
        forecast_date_to_target_date(x, lead) for x in ds[forecast_date_dim].values])
    return ds


def target_date_to_forecast_date(target_date, lead):
    """Converts a target date to a forecast date."""
    return _date_shift(target_date, lead, shift='backward')


def forecast_date_to_target_date(forecast_date, lead):
    """Converts a forecast date to a target date."""
    return _date_shift(forecast_date, lead, shift='forward')


def _date_shift(date, lead, shift='forward'):
    """Converts a target date to a forecast date or visa versa."""
    offset, offset_units = LEAD_OFFSETS[lead][1]
    input_type = type(date)

    # Convert input time to datetime object  for relative delta
    if isinstance(date, str):
        date_obj = dateparser.parse(date)
    elif isinstance(date, np.datetime64):
        date_obj = date.astype(datetime)
    elif isinstance(date, datetime):
        date_obj = date
    else:
        raise ValueError(f"Date type {type(date)} not supported.")

    # Shift the date
    if shift == 'forward':
        new_date = date_obj + relativedelta(**{offset_units: offset})
    elif shift == 'backward':
        new_date = date_obj - relativedelta(**{offset_units: offset})
    else:
        raise ValueError(f"Shift direction {shift} not supported")

    # Convert back to original type
    if np.issubdtype(input_type, str):
        new_date = datetime.strftime(new_date, "%Y-%m-%d")
    elif np.issubdtype(input_type, np.datetime64):
        new_date = np.datetime64(new_date, 'ns')
    return new_date


def add_dayofyear(ds, time_dim="time"):
    """Add a day of year coordinate to time dim, in the year 1904 (a leap year)."""
    ds = ds.assign_coords(dayofyear=(
        time_dim, pd.to_datetime([dateparser.parse(f"1904-{m}-{d}") for d, m in
                                  zip(ds[time_dim].dt.day.values, ds[time_dim].dt.month.values)])))
    return ds


def pad_with_leapdays(ds, time_dim="time"):
    """Pad the dataset with pseudo leap days in every year.

    Requires the input dataframe to have a dayofyear column. Modifies the input dataset.
    """
    # Find the years that don't have a leap day
    missing_leaps = [x for x in np.unique(ds[time_dim].dt.year.values) if not isleap(x)]
    missing_dates = [dateparser.parse(f"{x}-02-28") for x in missing_leaps]

    # Get the value on the 28th for these years
    missing_ds = ds.sel({time_dim: missing_dates})

    # Set the day of year to the 29th for these years
    missing_ds['dayofyear'] = (time_dim, [dateparser.parse("1904-02-29")]*len(missing_dates))

    # Add these new dates into the dataset and sort
    ds = xr.concat([ds, missing_ds], dim=time_dim)
    ds = ds.sortby(time_dim)
    return ds


def generate_dates_in_between(start_time, end_time, date_frequency, return_string=False):
    """Generates dates between two dates based on the frequency.

    Args:
        start_time (str): The start date.
        end_time (str): The end date.
        date_frequency (str): The frequency of the dates.
            One of "daily", "weekly", a day of the week (e.g., "Monday"), or a combination of days
            separated by a slash (e.g., "Monday/Thursday").
        return_string (bool): Whether to return the dates as strings or datetime objects.
    """
    start_date = dateparser.parse(start_time)
    end_date = dateparser.parse(end_time)

    if date_frequency not in ["daily", "weekly"]:
        dates = [
            date
            for date in generate_dates_in_between(start_time, end_time, "daily",
                                                  return_string=False)
            if date.strftime("%A") in date_frequency.split("/")
        ]
    else:
        frequency_to_int = {"daily": 1, "weekly": 7}
        dates = [
            start_date +
            timedelta(days=x * frequency_to_int[date_frequency])
            for x in range(0, int((end_date - start_date).days /
                                  (frequency_to_int[date_frequency])) + 1,)
        ]

    if return_string:
        dates = [date.strftime(DATETIME_FORMAT) for date in dates]
    return dates


def is_valid_forecast_date(model, forecast_type, forecast_date):
    """Checks if the forecast date is valid for the given model and forecast type."""
    valid_forecast_dates = {
        "reforecast": {
            "ecmwf": ("2015-05-14", datetime.today().strftime(DATETIME_FORMAT), "Monday/Thursday"),
            "salient": ("2022-01-01", datetime.today().strftime(DATETIME_FORMAT), "Wednesday"),
        },
        "forecast": {
            "ecmwf": ("2015-05-14", datetime.today().strftime(DATETIME_FORMAT), "Monday/Thursday"),
            "salient": ("2022-01-01", datetime.today().strftime(DATETIME_FORMAT), "Wednesday"),
        },
    }
    assert isinstance(forecast_date, datetime)
    try:
        return forecast_date in generate_dates_in_between(
            *valid_forecast_dates[forecast_type][model], return_string=False)
    except KeyError:
        return False


def get_dates(start_time, end_time, stride="day", return_string=True):
    """Outputs the list of dates corresponding to input date string."""
    # Input is of the form '20170101-20180130'
    start_date = dateparser.parse(start_time)
    end_date = dateparser.parse(end_time)

    if stride == "day":
        stride = DAILY
    elif stride == "week":
        stride = WEEKLY
    elif stride == "month":
        stride = MONTHLY
    elif stride == "year":
        stride = YEARLY
    else:
        raise ValueError(
            "Only day, week, month, and year strides are supported.")
    dates = [dt for dt in rrule(stride, dtstart=start_date, until=end_date)]
    if return_string:
        dates = [date.strftime(DATETIME_FORMAT) for date in dates]
    return dates
