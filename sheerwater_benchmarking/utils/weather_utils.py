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


def forecast(func):
    """Decorator to mark a function as a forecast and concat the leads."""
    # Register the forecast in the global forecast registry when defined
    FORECAST_REGISTRY[func.__name__] = func

    @wraps(func)
    def forecast_wrapper(*args, **kwargs):
        called_lead = kwargs.get('lead')
        leads, agg_period = get_leads(called_lead)
        try:
            ret = func(*args, **kwargs)
            ret = ret.assign_coords(lead_time=called_lead)
        except NotImplementedError:
            # Function does not implement the called lead, so we need to get the leads and agg period
            ret_list = []
            for lead in leads:
                kwargs['lead'] = lead
                try:
                    ds = func(*args, **kwargs)
                except NotImplementedError:
                    continue
                ds = ds.assign_coords(lead_time=lead)
                ret_list.append(ds)
            try:
                ret = xr.concat(ret_list, dim='lead_time')
            except ValueError as e:
                if 'must supply at least one object to concatenate' in str(e):
                    raise NotImplementedError(f"Lead {called_lead} not supported for {func.__name__}")
                else:
                    raise e
        # Assign agg period as time in seconds (timedeltas are not JSON serializable for storage)
        ret = ret.assign_attrs(agg_period_secs=float(agg_period / np.timedelta64(1, 's')))
        return ret
    return forecast_wrapper


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


def get_leads(lead):
    """Lead generation shortcuts.

    Support leads are 
    - weekly
    - biweekly
    - monthly
    - daily-n, where n is the number of days from day1 to dayn
    """
    if lead == 'weekly':
        return (['week1', 'week2', 'week3', 'week4', 'week5', 'week6'], np.timedelta64(7, 'D'))
    elif lead == 'biweekly':
        return (['weeks12', 'weeks23', 'weeks34', 'weeks45', 'weeks56'], np.timedelta64(14, 'D'))
    elif lead == 'monthly':
        return (['month1', 'month2', 'month3'], np.timedelta64(30, 'D'))
    elif 'daily' in lead:
        days = int(lead.split('-')[1])
        return ([f'day{i}' for i in range(1, days + 1)], np.timedelta64(1, 'D'))
    elif 'day' in lead:
        return ([lead], np.timedelta64(1, 'D'))
    elif 'week' in lead:
        return ([lead], np.timedelta64(7, 'D'))
    elif 'weeks' in lead:
        return ([lead], np.timedelta64(14, 'D'))
    elif 'month' in lead:
        return ([lead], np.timedelta64(30, 'D'))
    else:
        raise ValueError(f"Lead {lead} not supported")
