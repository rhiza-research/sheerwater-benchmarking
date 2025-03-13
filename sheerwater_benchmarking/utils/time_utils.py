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
    'daily': (1, (0, 'days')),
    'weekly': (7, (0, 'days')),
    'biweekly': (14, (0, 'days')),
    'monthly': (30, (0, 'days')),
    'month1': (30, (0, 'days')),
    'month2': (30, (30, 'days')),
    'month3': (30, (60, 'days')),
}
# Add daily and 8/11d windows
for i in range(46):
    LEAD_OFFSETS[f"day{i+1}"] = (1, (i, 'days'))
for i in [0, 7, 14, 21, 28, 35]:
    LEAD_OFFSETS[f"week{i//7+1}"] = (7, (i, 'days'))
for i in [0, 7, 14, 21, 28]:
    LEAD_OFFSETS[f"weeks{(i//7)+1}{(i//7)+2}"] = (14, (i, 'days'))


def lead_to_agg_days(lead):
    """Convert lead to time grouping."""
    return LEAD_OFFSETS[lead][0]


def lead_or_agg(lead):
    """Return whether argument is a lead or an agg."""
    if any(i.isdigit() for i in lead):
        return 'lead'
    else:
        return 'agg'


def dayofyear_to_datetime(x):
    """Converts day of year to datetime."""
    if np.isnan(x):
        return np.datetime64('NaT', 'ns')
    return np.datetime64("1904-01-01", 'ns') + np.timedelta64(int(x), 'D')


def assign_grouping_coordinates(ds, group, time_dim='time'):
    """Assigns time-based grouping coordinates to the time dimension.

    Args:
        ds (xr.Dataset): The dataset to add grouping to.
        group (str, list): The time grouping label to add. One of:
            - 'month': Group by month.
            - 'quarter': Group by quarter.
            - 'ea_rainy_season': Group by East African rainy season.
            - 'year': Group by year.
        time_dim (str): The time dimension to add the grouping based on.
    """
    # Ensure grouping is in standard format
    if group and not isinstance(group, list):
        group = [group]
    coords = []
    for grp in group:
        if grp == 'month':
            coords.append(ds[time_dim].dt.month.values)
        elif grp == 'quarter':
            coords.append(ds[time_dim].dt.quarter.values)
        elif grp == 'ea_rainy_season':
            # East African rainy period is from March to May and October to December
            def month_to_period(month):
                if 2 <= month <= 5:
                    return 'MAM'
                elif 9 <= month <= 12:
                    return 'OND'
                else:
                    return None
            coords.append([month_to_period(x) for x in ds[time_dim].dt.month.values])
        elif grp == 'year':
            coords.append(ds[time_dim].dt.year.values)
        else:
            raise ValueError(f"Invalid time grouping {grp}")

    def group_to_str(coord):
        coord_str = []  # string representation of the group
        for y in coord:  # iterate over all group values
            if y is None:  # if any values are None, give a none grouping value
                return None
            try:
                coord_str.append(f"{y:02d}")  # if the group value is a number, format it as a string
            except ValueError:  # MAM / OND strings
                coord_str.append(y)
        return "-".join(coord_str)  # join the group values with a dash

    joined_coords = [group_to_str(x) for x in zip(*coords)]
    ds = ds.assign_coords(group=(ds[time_dim].dims, joined_coords))
    return ds


def convert_group_to_time(group, groupby):
    """Converts a group label to a time coordinate.

    Assumes that groups are represented in the following format:
        - Years are a the year value
        - Months are 'M1', 'M2', ..., 'M12'
        - Quarters are 'Q1', 'Q2', ..., 'Q4'
        - East African rainy season is 'MAM', 'OND'

    Converts groups to datetime objects as follows:
        - Years are represented as the first day of the year
        - Months are represented as the first day of that month
        - Quarters are represented as the first day of the quarter
            (e.g., Q1 is Jan 1st, Q2 is Apr 1st, Q3 is Jul 1st, Q4 is Oct 1st)
        - East African rainy season is represented as the first day of the rainy season
            (e.g., the MAM rains are Mar 1st, the OND rains are Oct 1st)
    """
    def convert_to_datetime(entry, grouping):
        # Default values
        yy = '1904'
        mm = '01'
        dd = '01'
        if entry is None:
            return None
        for grp, val in zip(grouping, entry.split('-')):
            if grp == 'month':
                mm = f"{int(val):02d}"
            elif grp == 'quarter':
                mm = f"{(int(val)-1)*3+1:02d}"
            elif grp == 'ea_rainy_season':
                if val == 'MAM':
                    mm = '03'  # March rains
                elif val == 'OND':
                    mm = '10'  # October rains
                else:
                    raise ValueError(f"Invalid East African rainy season {val}")
            elif grp == 'year':
                yy = val
            else:
                raise ValueError(f"Invalid time grouping {grp}")
        return np.datetime64(f"{yy}-{mm}-{dd}", 'ns')
    # Ensure grouping is in standard format
    if groupby and not isinstance(groupby, list):
        groupby = [groupby]
    return [convert_to_datetime(x, groupby) for x in group.values]


def date_mean(data, dim='time'):
    """Mean of a date variables over a dimension, using integer number of days since 1970-01-01."""
    #  Get the number of days since 1970-01-01
    days = (data - np.datetime64('1970-01-01')).astype('timedelta64[D]').astype(int)
    days = days.where(days > -1000, np.nan)  # handle NaT, which is cast to a huge negative number
    days = days.mean(dim=dim, skipna=True)
    # Convert back to datetime, using integer number of days since 1970-01-01
    days = days.astype('datetime64[D]')
    return days


def doy_mean(data, dim='time'):
    """Mean of a date variables over a dimension, using day of year."""
    #  Get the number of days since 1970-01-01
    days = data.dt.dayofyear
    days = days.mean(dim=dim, skipna=True)
    # Convert back to datetime in the year 1904
    days = days.astype('datetime64[D]')
    days = days + (np.datetime64('1904-01-01') - np.datetime64('1970-01-01'))
    return days


def groupby_time(ds, groupby, agg_fn, time_dim='time', return_timeseries=False, **kwargs):
    """Aggregates data in groups along the time dimension according to time_grouping.

    Args:
        ds (xr.Dataset): The dataset to aggregate.
        groupby (str, list): The time grouping to use. Should be a string, a list, or a list of lists.
            List values should be one of:
            - 'month': Group by month.
            - 'year': Group by year.
            - 'quarter': Group by quarter.
            - 'ea_rainy_season': Group by East African rainy season (MAM and OND).
        agg_fn (object, list): The aggregation function to apply.
        time_dim (str): The time dimension to group by.
        return_timeseries (bool): If True, return a timeseries (the first date in each period).
             Otherwise, returns the label for the group (e.g., 'MAM', 2015, 'Q4', 'M1').
        kwargs: Additional keyword arguments to pass to the aggregation function.
    """
    # Input validation: if agg_fn is not a list, convert to a list
    if isinstance(groupby, list) and not isinstance(agg_fn, list):
        agg_fn = [agg_fn] * len(groupby)
    elif not isinstance(groupby, list) and not isinstance(agg_fn, list):
        groupby = [groupby]
        agg_fn = [agg_fn]
    elif not isinstance(groupby, list) and isinstance(agg_fn, list):
        raise ValueError("Cannot apply multiple aggregation functions to a single group.")
    if len(groupby) != len(agg_fn):
        raise ValueError("Length of group_by and agg_fn must be the same.")

    # Run multiple grouping steps
    for grp, agg in zip(groupby, agg_fn):
        # If no grouping is specified, apply the aggregation function directly
        if grp is None:
            ds = agg(ds, **kwargs)
        else:
            ds = assign_grouping_coordinates(ds, grp, time_dim)
            ds = ds.groupby("group").map(agg, **kwargs)
            if 'group' not in ds.dims:
                raise ValueError("Aggregation function must compress dataset along the group dimension.")

        # Convert group to time
        if (return_timeseries or len(groupby) > 1) and 'group' in ds.coords:
            ds = ds.assign_coords(time=("group", convert_group_to_time(ds['group'], grp)))
            ds = ds.swap_dims({'group': 'time'})
            ds = ds.sortby('time')
            ds = ds.drop('group')

    return ds


def shift_forecast_date_to_target_date(ds, forecast_date_dim, lead):
    """Shift a forecast date dimension to a target date coordinate from a lead time."""
    ds = ds.assign_coords({forecast_date_dim: [
        forecast_date_to_target_date(x, lead) for x in ds[forecast_date_dim].values]})
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
        date_obj = pd.Timestamp(date)
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
