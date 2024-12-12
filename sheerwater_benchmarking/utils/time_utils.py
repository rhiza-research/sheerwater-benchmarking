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


def assign_grouping_coordinates(ds, group, time_dim='time'):
    coords = []
    if not isinstance(group, list):
        group = [group]

    for grp in group:
        if grp == 'month':
            coords.append(ds[time_dim].dt.month.values)
        elif grp == 'quarter':
            coords.append(ds[time_dim].dt.quarter.values)
        elif grp == 'ea_rainy_season':
            # East African rainy period is from March to May and October to December
            def month_to_period(month):
                if 2 <= month <= 5:
                    return 1
                elif 8 <= month <= 12:
                    return 2
                else:
                    return 3
            coords.append([month_to_period(x) for x in ds[time_dim].dt.month.values])
        elif grp == 'year':
            coords.append(ds[time_dim].dt.year.values)
        else:
            raise ValueError("Invalid time grouping.")
    ds = ds.assign_coords(group=(time_dim, ["-".join([f"{y:02d}" for y in x]) for x in zip(*coords)]))
    return ds
    #     ds = ds.assign_coords(group=(time_dim,
    #                                  [f"E{month_to_period(m)}-{int(y.values)}" for m, y in
    #                                      zip(ds[time_dim].dt.month, ds[time_dim].dt.year)]))
    #     # East African rainy period is from March to May and October to December

    # elif group == 'quarter':
    #     ds = ds.assign_coords(group=(time_dim,
    #                                  [f"Q{int(q.values)}-{int(y.values)}" for q, y in
    #                                      zip(ds[time_dim].dt.quarter, ds[time_dim].dt.year)]))
    # elif group == 'ea_rainy_season':
    #     # East African rainy period is from March to May and October to December
    #     ds = ds.assign_coords(month=(time_dim, ds[time_dim].dt.month.values))
    #     # Drop values outside of the rainy seasons
    #     ds = ds.where((ds['month'] >= 2) & (ds['month'] <= 6) | (
    #         ds['month'] >= 9) & (ds['month'] <= 12), drop=True)
    #     ds = ds.drop('month')

    #     def month_to_period(month):
    #         return 1 if 2 <= month <= 6 else 2
    #     ds = ds.assign_coords(group=(time_dim,
    #                                  [f"E{month_to_period(m)}-{int(y.values)}" for m, y in
    #                                      zip(ds[time_dim].dt.month, ds[time_dim].dt.year)]))
    # elif group == 'year':
    #     ds = ds.assign_coords(group=(time_dim, ds[time_dim].dt.year.values))
    # else:
    #     raise ValueError("Invalid time grouping.")

    #         # ds.assign_coords(group=(time_dim,
    #         #                          [f"M{int(m.values)}-{int(y.values)}" for m, y in
    #         #                              zip(ds[time_dim].dt.month, ds[time_dim].dt.year)]))


def groupby_time(ds, groupby, agg_fn, time_dim='time', return_timeseries=False, only_schema=False, **kwargs):
    """Aggregates data in groups along the time dimension according to time_grouping.

    Args:
        ds (xr.Dataset): The dataset to aggregate.
        group_by (str, list): The time grouping to use. One of:
            - 'month': Group by month.
            - 'year': Group by year.
            - 'quarter': Group by quarter.
            - 'african_rainy_season': Group by African rainy season.
        agg_fn (object, list): The aggregation function to apply.
        time_dim (str): The time dimension to group by.
        return_timeseries (bool): If True, return a timeseries (the first date in each period). 
             Otherwise, returns the label for the group (e.g., 'January-2020', 12, 4).
    """
    # Input validation
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
            if only_schema:
                continue
            ds = agg(ds, **kwargs)
        else:
            ds = assign_grouping_coordinates(ds, grp, time_dim)
            if only_schema:
                # Get the first element of each group to create a schema
                ds = ds.groupby("group").first()
            else:
                ds = ds.groupby("group").map(agg, **kwargs)
            if 'group' not in ds.dims:
                raise ValueError("Aggregation function must compress dataset along the group dimension.")

        # Convert group to time
        if (return_timeseries or len(groupby) > 1) and 'group' in ds.coords:
            ds = ds.assign_coords(time=("group", convert_group_to_time(ds['group'], grp)))
            ds = ds.swap_dims({'group': 'time'})
            ds = ds.sortby('time')
            ds = ds.drop('group')

    # If we are aggregating across multiple periods, take the mean
    # if average_over is not None:
        # Convert to day of year to enable yearly averaging
        # ds = ds.rename({'group': 'subgroup'})
        # ds = ds.assign_coords(group=("subgroup", [x.split('-')[0][1:] for x in ds['subgroup'].values]))
        # ds = ds.groupby('group').mean(dim='subgroup', skipna=True)

        # Convert dayofyear back to datetime
        # TODO: couldn't get this to work with dask array (ValueError: cannot convert float NaN to integer
        # For now, compute and operate on the underlying np.array elementwise
        # ds = ds.dt.dayofyear
        # ds = xr.apply_ufunc(
        #     dayofyear_to_datetime,  # Function to apply
        #     ds.compute(),
        #     vectorize=True,  # Ensures element-wise operation
        #     output_dtypes=[np.datetime64]  # Specify output dtype
        # )

    # Convert group to time
    # if return_timeseries:
    #     ds = ds.assign_coords(time=("group", convert_group_to_time(ds['group'], group_by)))
    #     ds = ds.swap_dims({'group': 'time'})
    #     ds = ds.drop('group')
    #     ds = ds.sortby('time')
    return ds


def convert_group_to_time(group, grouping):
    """Converts a group label to a time coordinate."""
    def convert_to_datetime(entry, grouping):
        # Default values
        yy = '1904'
        mm = '01'
        dd = '01'
        for grp, val in zip(grouping, entry.split('-')):
            if grp == 'month':
                mm = f"{int(val):02d}"
            elif grp == 'quarter':
                mm = f"{(int(val)-1)*3+1:02d}"
            elif grp == 'ea_rainy_season':
                if val == '01':
                    mm = '03'  # March rains
                elif val == '02':
                    mm = '10'  # October rains
                elif val == '03':
                    mm = '07'  # Default to July for dry season
                else:
                    raise ValueError("Invalid East African rainy season")
            elif grp == 'year':
                yy = val
            else:
                raise ValueError("Invalid time grouping")
        return np.datetime64(f"{yy}-{mm}-{dd}", 'ns')
    if not isinstance(grouping, list):
        grouping = [grouping]
    return [convert_to_datetime(x, grouping) for x in group.values]

    # if grouping == 'month_of_year':
    #     return [np.datetime64(f"1904-{int(x):02d}-01", 'ns') for x in group.values]
    # elif grouping == 'month':
    #     return [np.datetime64(f"{x.split('-')[1]}-{int(x.split('-')[0][1:]):02d}-01", 'ns') for x in group.values]
    # elif grouping == 'year':
    #     return [np.datetime64(f"{int(x)}-01-01", 'ns') for x in group.values]
    # elif grouping == 'quarter_of_year':
    #     return [np.datetime64(f"1904-{(int(x)-1)*3+1:02d}-01", 'ns') for x in group.values]
    # elif grouping == 'quarter':
    #     return [np.datetime64(f"{x.split('-')[1]}-{(int(x.split('-')[0][1:])-1)*3+1:02d}-01", 'ns') for x in group.values]
    # elif grouping == 'ea_rainy_season_of_year':
    #     return [np.datetime64(f"1904-{(int(x)-1)*6+1:02d}-01", 'ns') for x in group.values]
    # elif grouping == 'ea_rainy_season':
    #     import pdb
    #     pdb.set_trace()
    #     return [np.datetime64(f"{x.split('-')[1]}-{(int(x.split('-')[0][1:])-1)*6+1:02d}-01", 'ns') for x in group.values]
    # else:
    #     raise ValueError("Invalid time grouping")


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
