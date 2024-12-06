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

LEAD_OFFSETS = {
    'day1': (0, 'days'),
    'day2': (1, 'days'),
    'day3': (2, 'days'),
    'day4': (3, 'days'),
    'day5': (4, 'days'),
    'day6': (5, 'days'),
    'day7': (6, 'days'),
    'day8': (7, 'days'),
    'day9': (8, 'days'),
    'day10': (9, 'days'),
    'day11': (10, 'days'),
    'day12': (11, 'days'),
    'day13': (12, 'days'),
    'day14': (13, 'days'),
    'day15': (14, 'days'),
    'day16': (15, 'days'),
    'day17': (16, 'days'),
    'day18': (17, 'days'),
    'day19': (18, 'days'),
    'day20': (19, 'days'),
    'day21': (20, 'days'),
    'day22': (21, 'days'),
    'day23': (22, 'days'),
    'day24': (23, 'days'),
    'day25': (24, 'days'),
    'day26': (25, 'days'),
    'day27': (26, 'days'),
    'day28': (27, 'days'),
    'day29': (28, 'days'),
    'day30': (29, 'days'),
    'day31': (30, 'days'),
    'day32': (31, 'days'),
    'day33': (32, 'days'),
    'day34': (33, 'days'),
    'day35': (34, 'days'),
    'day36': (35, 'days'),
    'day37': (36, 'days'),
    'day38': (37, 'days'),
    'day39': (38, 'days'),
    'day40': (39, 'days'),
    'day41': (40, 'days'),
    'day42': (41, 'days'),
    'day43': (42, 'days'),
    'day44': (43, 'days'),
    'day45': (44, 'days'),
    'day46': (45, 'days'),
    'week1': (0, 'days'),
    'week2': (7, 'days'),
    'week3': (14, 'days'),
    'week4': (21, 'days'),
    'week5': (28, 'days'),
    'week6': (35, 'days'),
    'weeks12': (0, 'days'),
    'weeks34': (14, 'days'),
    'weeks56': (28, 'days'),
    'month1': (0, 'months'),
    'month2': (1, 'months'),
    'month3': (2, 'months'),
    'quarter1': (0, 'months'),
    'quarter2': (3, 'months'),
    'quarter3': (6, 'months'),
    'quarter4': (9, 'months'),
}


def lead_to_time_group(lead):
    """Convert lead to time grouping."""
    if 'weeks' in lead:
        return 'biweekly'
    elif 'week' in lead:
        return 'weekly'
    elif 'month' in lead:
        return 'monthly'
    elif 'quarter' in lead:
        return 'quarterly'
    else:
        raise ValueError(f"Unknown lead {lead}")


def groupby_time(ds, grouping, agg_fn, time_dim='time', return_timeseries=False, **kwargs):
    """Aggregates data in groups along the time dimension according to time_grouping.

    Args:
        ds (xr.Dataset): The dataset to aggregate.
        time_grouping (str): The time grouping to use. One of:
            - 'month_of_year': Group by month of year.
            - 'month': Group by month.
            - 'year': Group by year.
            - 'quarter_of_year': Group by quarter of year.
            - 'quarter': Group by quarter.
        agg_fn (object): The aggregation function to apply.
        time_dim (str): The time dimension to group by.
        return_timeseries (bool): If True, return a timeseries (the first date in each period). 
             Otherwise, returns the label for the group (e.g., 'January-2020', 12, 4).
    """
    if grouping is None:
        return ds.map(agg_fn, **kwargs)

    if grouping == 'month_of_year':
        ds = ds.assign_coords(group=("time", ds[time_dim].dt.month))
    elif grouping == 'month':
        ds = ds.assign_coords(group=("time",
                                     [f"M{int(m.values)}-{int(y.values)}" for m, y in
                                      zip(ds[time_dim].dt.month, ds[time_dim].dt.year)]))
    elif grouping == 'year':
        ds = ds.assign_coords(group=("time", ds[time_dim].dt.year))
    elif grouping == 'quarter_of_year':
        ds = ds.assign_coords(group=("time", ds[time_dim].dt.quarter))
    elif grouping == 'quarter':
        ds = ds.assign_coords(group=("time",
                                     [f"Q{int(q.values)}-{int(y.values)}" for q, y in
                                      zip(ds[time_dim].dt.quarter, ds[time_dim].dt.year)]))
    else:
        raise ValueError("Invalid time grouping")

    ds = ds.groupby("group").map(agg_fn, **kwargs)
    if return_timeseries:
        ds = ds.assign_coords(time=("group", convert_group_to_time(ds['group'], grouping)))
        ds = ds.swap_dims({'group': 'time'})
        ds = ds.drop('group')
        ds = ds.sortby('time')
    return ds


def convert_group_to_time(group, grouping):
    """Converts a group label to a time coordinate."""
    if grouping == 'month_of_year':
        return [np.datetime64(f"1904-{int(x):02d}-01", 'ns') for x in group.values]
    elif grouping == 'month':
        return [np.datetime64(f"{x.split('-')[1]}-{int(x.split('-')[0][1:]):02d}-01", 'ns') for x in group.values]
    elif grouping == 'year':
        return [np.datetime64(f"{int(x)}-01-01", 'ns') for x in group.values]
    elif grouping == 'quarter_of_year':
        return [np.datetime64(f"1904-{(int(x)-1)*3+1:02d}-01", 'ns') for x in group.values]
    elif grouping == 'quarter':
        return [np.datetime64(f"{x.split('-')[1]}-{(int(x.split('-')[0][1:])-1)*3+1:02d}-01", 'ns') for x in group.values]
    else:
        raise ValueError("Invalid time grouping")


def convert_to_target_date_dim(ds, forecast_date_dim, lead):
    """Creates a target date dimension `time` from a forecast date coordinate and lead time."""
    ds = ds.assign_coords(time=(forecast_date_dim, [np.datetime64(forecast_date_to_target_date(x, lead, return_string=False), 'ns')
                                for x in ds[forecast_date_dim].values]))
    ds = ds.swap_dims({forecast_date_dim: 'time'})
    ds = ds.drop(forecast_date_dim)
    return ds


def target_date_to_forecast_date(target_date, lead, return_string=True):
    """Converts a target date to a forecast date."""
    return _date_shift(target_date, lead, add=False, return_string=return_string)


def forecast_date_to_target_date(forecast_date, lead, return_string=True):
    """Converts a forecast date to a target date."""
    return _date_shift(forecast_date, lead, add=True, return_string=return_string)


def _date_shift(date, lead, add=False, return_string=True):
    """Converts a target date to a forecast date or visa versa."""
    offset, offset_units = LEAD_OFFSETS[lead]
    if isinstance(date, str):
        date_obj = dateparser.parse(date)
    else:
        date_obj = date.astype('M8[D]').astype('O')
    if add:
        new_date = date_obj + relativedelta(**{offset_units: offset})
    else:
        new_date = date_obj - relativedelta(**{offset_units: offset})

    if return_string:
        new_date = datetime.strftime(new_date, "%Y-%m-%d")
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
