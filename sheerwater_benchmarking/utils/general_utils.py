"""General utility functions for all parts of the data pipeline."""
import time

import numpy as np

import requests
import ssl
import gcsfs
import xarray as xr
from urllib3 import poolmanager
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY, MONTHLY, WEEKLY, YEARLY


DATETIME_FORMAT = "%Y-%m-%d"


def load_netcdf(filepath):
    """Load a NetCDF dataset from cloud bucket."""
    # Load the dataset
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    netc = fs.open(filepath)
    ds = xr.open_dataset(netc, engine="h5netcdf")
    return ds


def write_zarr(ds, filepath):
    """Write an xarray to a Zarr file in cloud bucket."""
    # Load the dataset
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    _ = fs.open(filepath)
    # write it back out to ZARR
    gcsmap = fs.get_mapper(filepath)
    ds.to_zarr(store=gcsmap, mode='w')


def load_zarr(filename):
    """Load a Zarr dataset from cloud bucket."""
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    cache_map = fs.get_mapper(filename)
    ds = xr.open_dataset(cache_map, engine='zarr')
    return ds


def string_to_dt(string):
    """Transforms string to datetime."""
    return datetime.strptime(string, DATETIME_FORMAT)


def dt_to_string(dt):
    """Transforms datetime to string."""
    return datetime.strftime(dt, DATETIME_FORMAT)


valid_forecast_dates = {
    "reforecast": {
        "ecmwf": (string_to_dt("2015-05-14"), datetime.today(), "monday/thursday"),
    },
    "forecast": {
        "ecmwf": (string_to_dt("2015-05-14"), datetime.today(), "monday/thursday"),
    },
}


def is_valid_forecast_date(model, forecast_type, forecast_date):
    """Checks if the forecast date is valid for the given model and forecast type."""
    assert isinstance(forecast_date, datetime)
    try:
        return forecast_date in generate_dates_in_between(
            *valid_forecast_dates[forecast_type][model])
    except KeyError:
        return False


def generate_dates_in_between(first_date, last_date, date_frequency):
    """Generates dates between two dates based on the frequency.

    Args:
        first_date (datetime): The first date.
        last_date (datetime): The last date.
        date_frequency (str): The frequency of the dates.
            One of "daily", "weekly", "monday/thursday".
    """
    if date_frequency == "monday/thursday":
        dates = [
            date
            for date in generate_dates_in_between(first_date, last_date, "daily")
            if date.strftime("%A") in ["Monday", "Thursday"]
        ]
        return dates
    else:
        frequency_to_int = {"daily": 1, "weekly": 7}
        dates = [
            first_date +
            timedelta(days=x * frequency_to_int[date_frequency])
            for x in range(0, int((last_date - first_date).days /
                                  (frequency_to_int[date_frequency])) + 1,)
        ]
        return dates


def printf(str):
    """Calls print on given argument and then flushes stdout buffer.

    Ensures that printed message is displayed right away
    """
    print(str, flush=True)


def print_fail(message="FAIL", verbose=True, skip_line_before=True,
               skip_line_after=True, bold=False):
    """Print message in purple."""
    if verbose:
        string_before = "\n" if skip_line_before else ""
        string_after = "\n" if skip_line_after else ""
        if bold:
            print(
                f"{string_before}\x1b[1;30;45m[ {message} ]\x1b[0m{string_after}")
        else:
            print(f"{string_before}\x1b[35m{message}\x1b[0m{string_after}")


def print_error(message="ERROR", verbose=True, skip_line_before=True,
                skip_line_after=True, bold=False):
    """Print message in red."""
    if verbose:
        string_before = "\n" if skip_line_before else ""
        string_after = "\n" if skip_line_after else ""
        if bold:
            print(
                f"{string_before}\x1b[1;30;41m[ {message} ]\x1b[0m{string_after}")
        else:
            print(f"{string_before}\x1b[31m{message}\x1b[0m{string_after}")


def print_warning(message="WARNING", verbose=True, skip_line_before=True,
                  skip_line_after=True, bold=False):
    """Print message in yellow."""
    if verbose:
        string_before = "\n" if skip_line_before else ""
        string_after = "\n" if skip_line_after else ""
        if bold:
            print(
                f"{string_before}\x1b[1;30;43m[ {message} ]\x1b[0m{string_after}")
        else:
            print(f"{string_before}\x1b[33m{message}\x1b[0m{string_after}")


def print_ok(message="OK", verbose=True, skip_line_before=True, skip_line_after=True, bold=False):
    """Print message in green."""
    if verbose:
        string_before = "\n" if skip_line_before else ""
        string_after = "\n" if skip_line_after else ""
        if bold:
            print(
                f"{string_before}\x1b[1;30;42m[ {message} ]\x1b[0m{string_after}")
        else:
            print(f"{string_before}\x1b[32m{message}\x1b[0m{string_after}")


def print_info(message, verbose=True):
    """Print message if verbose."""
    if verbose:
        print(message)


class TLSAdapter(requests.adapters.HTTPAdapter):
    """Transport adapter that allows us to use TLSv1.2."""

    def init_poolmanager(self, connections, maxsize, block=False):
        """Create and initialize the urllib3 PoolManager."""
        ctx = ssl.create_default_context()
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        self.poolmanager = poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_version=ssl.PROTOCOL_TLS,
            ssl_context=ctx)


def download_url(url, timeout=600, retry=3, cookies={}):
    """Download URL, waiting some time between retries."""
    r = None
    session = requests.session()
    session.mount('https://', TLSAdapter())

    for i in range(retry):
        try:
            r = session.get(url, timeout=timeout, cookies=cookies)
            return r
        except requests.exceptions.Timeout as e:
            # Wait until making another request
            if i == retry - 1:
                raise e
            print(f"Request to url {url} has timed out. Trying again...")
            time.sleep(3)
    print(f"Failed to retrieve file after {retry} attempts. Stopping...")


def get_dates(start_time, end_time, stride="day", return_string=False):
    """Outputs the list of dates corresponding to input date string."""
    # Input is of the form '20170101-20180130'
    start_date = datetime.strptime(start_time, DATETIME_FORMAT)
    end_date = datetime.strptime(end_time, DATETIME_FORMAT)

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


def get_variable(variable_name, variable_type='era5'):
    """Converts a variable in any other type to a variable name of the requested type."""
    variable_ordering = ['sheerwater', 'era5']

    weather_variables = [
        # Static variables (2):
        ('z', 'geopotential'),
        ('lsm', 'land_sea_mask'),

        # Surface variables (6):
        ('tmp2m', '2m_temperature'),
        ("precip", "total_precipitation"),
        ("vwind10m", "10m_v_component_of_wind"),
        ("uwind10m", "10m_u_component_of_wind"),
        ("msl", "mean_sea_level_pressure"),
        ("tisr", "toa_incident_solar_radiation"),

        # Atmospheric variables (6):
        ("tmp", "temperature"),
        ("uwind", "u_component_of_wind"),
        ("vwind", "v_component_of_wind"),
        ("hgt", "geopotential"),
        ("q", "specific_humidity"),
        ("w", "vertical_velocity"),
    ]

    name_index = variable_ordering.index(variable_type)

    for tup in weather_variables:
        for name in tup:
            if name == variable_name:
                return tup[name_index]

    raise ValueError(f"Variable {variable_name} not found")


def get_grid(region_id):
    """Get the longitudes, latitudes and grid size for a named region."""
    if region_id == "global1_5":
        longitudes = np.arange(0, 360, 1.5)
        latitudes = np.arange(-90, 90, 1.5)
        grid_size = 1.5
    elif region_id == "global0_5":
        longitudes = np.arange(0.25, 360, 0.5)
        latitudes = np.arange(-89.75, 90, 0.5)
        grid_size = 0.5
    elif region_id == "global0_25":
        longitudes = np.arange(0, 360, 0.25)
        latitudes = np.arange(-90, 90, 0.25)
        grid_size = 0.25
    elif region_id == "us1_0":
        longitudes = np.arange(-125.0, -67.0, 1)
        latitudes = np.arange(25.0, 50.0, 1)
        grid_size = 1.0
    elif region_id == "us1_5":
        longitudes = np.arange(-123.0, -67.5, 1.5)
        latitudes = np.arange(25.5, 48, 1.5)
        grid_size = 1.5
    elif region_id == "salient_common":
        longitudes = np.arange(0.125, 360, 0.25)
        latitudes = np.arange(-89.875, 90, 0.25)
        grid_size = 0.25
    else:
        raise NotImplementedError(
            "Only grids global1_5, us1_0 and us1_5 have been implemented.")
    return longitudes, latitudes, grid_size
