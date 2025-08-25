"""Variable-related utility functions for all parts of the data pipeline."""

from functools import wraps
import xarray as xr
import numpy as np

# Global forecast registry
FORECAST_REGISTRY = {}


def forecast(func):
    """Decorator to mark a function as a forecast and concat the leads."""
    # Register the forecast in the global forecast registry when defined
    FORECAST_REGISTRY[func.__name__] = func

    @wraps(func)
    def forecast_wrapper(*args, **kwargs):
        called_lead = kwargs.get('lead')
        leads, agg_period = get_leads(called_lead)
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
        ret = ret.assign_attrs(agg_period=agg_period)
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
        return ([f'day{i}' for i in range(1, days + 1)], np.timedelta64(days, 'D'))
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
