"""Variable-related utility functions for all parts of the data pipeline."""

from functools import wraps
import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dateparser

# Global forecast registry
FORECAST_REGISTRY = {}


def forecast(func):
    """Decorator to mark a function as a forecast and concat the leads."""
    # Register the forecast in the global forecast registry when defined
    FORECAST_REGISTRY[func.__name__] = func

    @wraps(func)
    def forecast_wrapper(*args, **kwargs):
        called_lead = kwargs.get('lead')
        lead_group = get_lead_group(called_lead)
        agg_period = get_lead_info(lead_group)['agg_period']

        # Overwrite the called lead with the lead group
        kwargs['lead'] = lead_group
        ds = func(*args, **kwargs)
        # Assign agg period as time in seconds (timedeltas are not JSON serializable for storage)
        ds = ds.assign_attrs(agg_period_secs=float(agg_period / np.timedelta64(1, 's')))

        # Filter to the called lead
        if called_lead != lead_group:
            ds = ds.sel(lead_time=called_lead)
        return ds
    return forecast_wrapper


def lead_or_agg(lead):
    """Return whether argument is a lead or an agg."""
    if any(i.isdigit() for i in lead) or lead in ['daily', 'weekly', 'biweekly', 'monthly']:
        return 'lead'
    else:
        return 'agg'


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
    offset_timedelta = get_lead_info(lead)['lead_offsets']
    assert len(offset_timedelta) == 1, "Only one lead offset is supported for date shifting"
    offset_timedelta = offset_timedelta[0]
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
        new_date = date_obj + offset_timedelta
    elif shift == 'backward':
        new_date = date_obj - offset_timedelta
    else:
        raise ValueError(f"Shift direction {shift} not supported")

    # Convert back to original type
    if np.issubdtype(input_type, str):
        new_date = datetime.strftime(new_date, "%Y-%m-%d")
    elif np.issubdtype(input_type, np.datetime64):
        new_date = np.datetime64(new_date, 'ns')
    return new_date


def convert_lead_to_valid_time(ds, initialization_date_dim='start_date', lead_time_dim='lead_time', valid_time_dim='time'):
    """Convert the start_date and lead_time coordinates to a valid_time coordinate."""
    ds = ds.assign_coords({valid_time_dim: ds[initialization_date_dim] + ds[lead_time_dim]})
    tmp = ds.stack(z=(initialization_date_dim, lead_time_dim))
    tmp = tmp.set_index(z=(valid_time_dim, lead_time_dim))
    ds = tmp.unstack('z')
    return ds


def get_forecast(forecast_name):
    """Get a forecast from the global forecast registry."""
    return FORECAST_REGISTRY[forecast_name]


def get_variable(variable_name, variable_type='era5'):
    """Converts a variable in any other type to a variable name of the requested type."""
    variable_ordering = ['sheerwater', 'era5', 'ecmwf_hres', 'ecmwf_ifs_er', 'salient', 'abc', 'ghcn']

    weather_variables = [
        # Static variables (2):
        ('z', 'geopotential', 'geopotential', None, None, None, None),
        ('lsm', 'land_sea_mask', 'land_sea_mask', None, None, None, None),

        # Surface variables (6):
        ('tmp2m', '2m_temperature', '2m_temperature', '2m_temperature', 'temp', 'tmp2m', 'temp'),
        ('precip', 'total_precipitation', 'total_precipitation_6hr', 'total_precipitation_24hr',
         'precip', 'precip', 'precip'),
        ("vwind10m", "10m_v_component_of_wind", "10m_v_component_of_wind", None, None, None, None),
        ("uwind10m", "10m_u_component_of_wind", "10m_u_component_of_wind", None, None, None, None),
        ("msl", "mean_sea_level_pressure", "mean_sea_level_pressure", None, None, None, None),
        ("tisr", "toa_incident_solar_radiation", "toa_incident_solar_radiation", None, "tsi", None, None),
        ("ssrd", "surface_solar_radiation_downwards", None, None, "tsi", None, None),

        # Atmospheric variables (6):
        ("tmp", "temperature", "temperature", None, None, None, None),
        ("uwind", "u_component_of_wind", "u_component_of_wind", None, None, None, None),
        ("vwind", "v_component_of_wind", "v_component_of_wind", None, None, None, None),
        ("hgt", "geopotential", "geopotential", None, None, None, None),
        ("q", "specific_humidity", "specific_humidity", None, None, None, None),
        ("w", "vertical_velocity", "vertical_velocity", None, None, None, None),
    ]

    name_index = variable_ordering.index(variable_type)

    for tup in weather_variables:
        for name in tup:
            if name == variable_name:
                val = tup[name_index]
                if val is None:
                    raise ValueError(f"Variable {variable_name} not implemented.")
                return val

    raise ValueError(f"Variable {variable_name} not found")


def get_lead_group(lead):
    """Get the lead group for a lead."""
    if lead == 'weekly':
        return 'weekly'
    elif lead == 'biweekly':
        return 'biweekly'
    elif lead == 'monthly':
        return 'monthly'
    elif lead == 'quarterly':
        return 'quarterly'
    elif 'daily' in lead:
        return 'daily'
    elif 'day' in lead:
        return 'daily'
    elif 'weeks' in lead:
        return 'biweekly'
    elif 'week' in lead and 'weeks' not in lead:
        return 'weekly'
    elif 'month' in lead:
        return 'monthly'
    elif 'quarter' in lead:
        return 'quarterly'
    else:
        raise ValueError(f"Lead {lead} not supported")


def get_lead_info(lead):
    """Get lead information.

    Support leads are 
    - weekly
    - biweekly
    - monthly
    - quarterly
    - daily-n, where n is the number of days from day1 to dayn
    - week1, week2, week3, week4, week5, week6
    - weeks12, weeks23, weeks34, weeks45, weeks56
    - month1, month2, month3
    """
    if lead == 'weekly':
        return {
            'agg_period': np.timedelta64(7, 'D'),
            'agg_days': 7,
            'lead_offsets': [np.timedelta64(0, 'D'), np.timedelta64(7, 'D'), np.timedelta64(14, 'D'), np.timedelta64(21, 'D'), np.timedelta64(28, 'D'), np.timedelta64(35, 'D')],
            'labels': ['week1', 'week2', 'week3', 'week4', 'week5', 'week6'],
        }
    elif lead == 'biweekly':
        return {
            'agg_period': np.timedelta64(14, 'D'),
            'agg_days': 14,
            'lead_offsets': [np.timedelta64(0, 'D'), np.timedelta64(7, 'D'), np.timedelta64(14, 'D'), np.timedelta64(21, 'D'), np.timedelta64(28, 'D')],
            'labels': ['weeks12', 'weeks23', 'weeks34', 'weeks45', 'weeks56'],
        }
    elif lead == 'monthly':
        return {
            'agg_period': np.timedelta64(30, 'D'),
            'agg_days': 30,
            'lead_offsets': [np.timedelta64(0, 'D'), np.timedelta64(30, 'D'), np.timedelta64(60, 'D')],
            'labels': ['month1', 'month2', 'month3'],
        }
    elif lead == 'quarterly':
        return {
            'agg_period': np.timedelta64(90, 'D'),
            'agg_days': 90,
            'lead_offsets': [np.timedelta64(0, 'D'), np.timedelta64(90, 'D'), np.timedelta64(180, 'D'), np.timedelta64(270, 'D')],
            'labels': ['quarter1', 'quarter2', 'quarter3', 'quarter4'],
        }
    elif 'daily' in lead:
        days = int(lead.split('-')[1])
        if days < 1 or days > 366:
            raise ValueError(f"Daily lead {lead} must be between 1 and 366 days, got {days}")
        return {
            'agg_period': np.timedelta64(1, 'D'),
            'agg_days': 1,
            'lead_offsets': [np.timedelta64(i, 'D') for i in range(days)],
            'labels': [f'day{i}' for i in range(1, days + 1)],
        }
    elif 'day' in lead:
        day_num = int(lead[3:])
        if day_num < 1 or day_num > 366:
            raise ValueError(f"Day lead {lead} must be between day1 and day366, got day{day_num}")
        return {
            'agg_period': np.timedelta64(1, 'D'),
            'agg_days': 1,
            'labels': [lead],
            'lead_offsets': [np.timedelta64(day_num, 'D')],
        }
    elif 'week' in lead and 'weeks' not in lead:
        week_num = int(lead[4:])
        if week_num < 1 or week_num > 52:
            raise ValueError(f"Week lead {lead} must be between week1 and week52, got week{week_num}")
        return {
            'agg_period': np.timedelta64(7, 'D'),
            'agg_days': 7,
            'labels': [lead],
            'lead_offsets': [np.timedelta64((week_num+1)*7, 'D')],
        }
    elif 'weeks' in lead:
        week_num = int(lead[5])
        second_week_num = int(lead[6])
        if second_week_num != week_num + 1:
            raise ValueError(
                f"Biweekly lead {lead} must be between week{week_num} and week{week_num+1}, got weeks{week_num}{second_week_num}")
        if week_num < 1 or week_num > 52 or second_week_num < 1 or second_week_num > 52:
            raise ValueError(f"Biweekly lead {lead} must be between weeks12 and weeks52, got weeks{week_num}")
        return {
            'agg_period': np.timedelta64(14, 'D'),
            'agg_days': 14,
            'labels': [lead],
            'lead_offsets': [np.timedelta64((week_num-1)*7, 'D')],
        }
    elif 'month' in lead:
        month_num = int(lead[5:])
        if month_num < 1 or month_num > 12:
            raise ValueError(f"Month lead {lead} must be between month1 and month12, got month{month_num}")
        return {
            'agg_period': np.timedelta64(30, 'D'),
            'agg_days': 30,
            'labels': [lead],
            'lead_offsets': [np.timedelta64(month_num*30, 'D')],
        }
    elif 'quarter' in lead:
        quarter_num = int(lead[7:])
        if quarter_num < 1 or quarter_num > 4:
            raise ValueError(f"Quarter lead {lead} must be between quarter1 and quarter4, got quarter{quarter_num}")
        return {
            'agg_period': np.timedelta64(90, 'D'),
            'agg_days': 90,
            'labels': [lead],
            'lead_offsets': [np.timedelta64(quarter_num*90, 'D')],
        }
    else:
        raise ValueError(f"Lead {lead} not supported")
