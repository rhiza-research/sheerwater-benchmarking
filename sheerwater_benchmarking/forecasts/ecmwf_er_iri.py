"""Functions to fetch and process data from the ECMWF IRI dataset.

NOTE: these are legacy functions and no longer actively maintained.
"""
import sys
import pandas as pd
from datetime import datetime
import dateparser
import os
import xarray as xr
import requests
import ssl
from urllib3 import poolmanager
import time
import dask

from sheerwater_benchmarking.utils import (dask_remote, cacheable, ecmwf_secret,
                                           get_grid, get_dates,
                                           is_valid_forecast_date,
                                           roll_and_agg,
                                           lon_base_change,
                                           regrid)


########################################################################
# IRI download utility functions
########################################################################
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


########################################################################
#  IRI ECMWF download and process functions
########################################################################
@dask_remote
@cacheable(data_type='array',
           cache_args=['time', 'variable', 'forecast_type', 'run_type', 'grid'],
           chunking={'lat': 121, 'lon': 240, 'lead_time': 46, 'model_run': 1,
                     'start_date': 969, 'model_issuance_date': 1})
def single_iri_ecmwf(time, variable, forecast_type,
                     run_type="average", grid="global1_5",
                     verbose=True):
    """Fetches forecast data from the IRI ECMWF dataset.

    Args:
        time (str): The date to fetch data for (by day).
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "reforecast".
        run_type (str): The type of run to fetch. One of:
            - average: to download the averaged of the perturbed runs
            - control: to download the control forecast
            - perturbed: to download all perturbed runs
            - [int 0-50]: to download a specific  perturbed run
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
        verbose (bool): Whether to print verbose output.
    """
    if variable == "tmp2m":
        weather_variable_name_on_server = "2m_above_ground/.2t"
    elif variable == "precip":
        weather_variable_name_on_server = "sfc_precip/.tp"
    else:
        raise ValueError("Invalid weather variable.")

    forecast_runs = "control" if run_type == "control" else "perturbed"
    leads_id = "LA" if variable == "tmp2m" else "L"
    average_model_runs_url = "[M]average/" if run_type == "average" else ""
    single_model_run_url = f"M/({run_type})VALUES/" if isinstance(run_type, int) else ""

    lons, lats, grid_size = get_grid(grid)
    restrict_lat_url = f"Y/{lats[0]}/{grid_size}/{lats[-1]}/GRID/"
    restrict_lon_url = f"X/{lons[0]}/{grid_size}/{lons[-1]}/GRID/"

    if variable == "tmp2m":
        # Convert from Kelvin to Celsius.
        differences_url = ""
        convert_units_url = "(Celsius_scale)/unitconvert/"
    else:
        differences_url = "[L]differences/"
        convert_units_url = "c://name//water_density/def/998/(kg/m3)/:c/div/(mm)/unitconvert/"

    if verbose:
        print("ecmwf")

    date = dateparser.parse(time)
    day, month, year = datetime.strftime(date, "%d,%b,%Y").split(",")

    restrict_forecast_date_url = f"S/({day} {month} {year})VALUES/"

    if not is_valid_forecast_date("ecmwf", forecast_type, date):
        if verbose:
            print(
                f"Skipping: {day} {month} {year} (not a valid {forecast_type} date)."
            )
        return None

    URL = (
        f"https://iridl.ldeo.columbia.edu/SOURCES/.ECMWF/.S2S/.ECMF/"
        f".{forecast_type}/.{forecast_runs}/.{weather_variable_name_on_server}/"
        f"{average_model_runs_url}"
        f"{single_model_run_url}"
        f"{restrict_forecast_date_url}"
        f"{restrict_lat_url}"
        f"{restrict_lon_url}"
        f"{convert_units_url}"
        f"{differences_url}"
        f"data.nc"
    )

    os.makedirs('./temp', exist_ok=True)
    file = f"./temp/{variable}-{grid}-{run_type}-{forecast_type}-{time}.nc"
    r = download_url(URL, cookies={"__dlauth_id": ecmwf_secret()})
    if r.status_code == 200 and r.headers["Content-Type"] == "application/x-netcdf":
        if verbose:
            print(f"Downloading: {day} {month} {year}.")
        with open(file, "wb") as f:
            f.write(r.content)

        if verbose:
            print(f"-done (downloaded {sys.getsizeof(r.content) / 1024:.2f} KB).\n")
    elif r.status_code == 404:
        print(f"Data for {day} {month} {year} is not available for model ecmwf.\n")
        return None
    else:
        raise ValueError(f"Failed to download data for {day} {month} {year} for model ecmwf.")

    rename_dict = {
        "S": "start_date",
        f"{leads_id}": "lead_time",
        "X": "lon",
        "Y": "lat",
    }

    if variable == "precip":
        rename_dict["ratio"] = "precip"
    else:
        rename_dict["2t"] = "tmp2m"

    try:
        # Read the data and return individual datasets
        if forecast_type == "forecast":
            ds = xr.open_dataset(file, engine="netcdf4")
        else:
            ds = xr.open_dataset(file, decode_times=False, engine="netcdf4")

            # Manually decode the time variable
            ds['S'] = pd.to_datetime(
                ds['S'].values, unit="D", origin=pd.Timestamp("1960-01-01"))

            model_issuance_day = ds['S.day'].values[0]
            model_issuance_month = ds['S.month'].values[0]
            model_issuance_date_in_1960 = pd.Timestamp(
                f"1960-{model_issuance_month}-{model_issuance_day}")

            # While hdates refer to years (for which the model issued in ds["S"] is initialized),
            # its values are given as months until the middle of the year, so 6 months are subtracted
            # to yield the beginning of the year.
            ds['hdate'] = pd.to_datetime(
                [model_issuance_date_in_1960 + pd.DateOffset(months=x-6) for x in ds['hdate'].values])

            # Drop future forecast dates, which are all NaNs
            ds = ds.dropna(dim="hdate", how="all")
            if ds.sizes["hdate"] == 0:
                # If no forecast dates are available, return None
                print(f"No data found for: {day} {month} {year} (a valid {forecast_type} date).")
                return None

            # Reforecast-specific renaming
            rename_dict["hdate"] = "start_date"
            rename_dict["S"] = "model_issuance_date"
    except OSError:
        print(f"Failed to load data for: {day} {month} {year} (a valid {forecast_type} date).")
        return None

    # Deal with model runs
    if "M" in ds and ds.sizes["M"] == 1:
        ds = ds.squeeze("M")
    elif "M" in ds:
        rename_dict["M"] = "model_run"

    # Rename columns to standard names
    ds = ds.rename(rename_dict)

    ds = ds.compute()
    os.remove(file)
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['time', 'variable', 'forecast_type', 'run_type', 'grid'],
           chunking={'lat': 121, 'lon': 240, 'lead_time': 46, 'model_run': 1,
                     'start_year': 20, 'model_issuance_date': 1})
def single_iri_ecmwf_dense(time, variable, forecast_type,
                           run_type="average", grid="global1_5",
                           verbose=True):
    """Fetches a single IRI ECMWF forecast and then converts to dense format.

    Converts the start_date to start date year. This allows a dense array post merging because
    the data within the year is always the same as the model issuance date.
    eliminating the sparsity of start_dates without model issuance dates.

    Interface is the same as single_iri_ecmwf.
    """
    ds = single_iri_ecmwf(time, variable, forecast_type, run_type, grid, verbose)

    if ds is None:
        return None

    # Convert start_date to start_year
    ds = ds.assign_coords(start_date=ds['start_date.year'])
    ds = ds.rename({'start_date': 'start_year'})

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries=['start_date', 'model_issuance_date'],
           cache_args=['variable', 'forecast_type', 'run_type', 'grid'],
           chunking={'lat': 121, 'lon': 240, 'lead_time': 46, 'start_date': 969,
                     'model_run': 1, 'start_year': 29, 'model_issuance_date': 1},
           cache=False,
           auto_rechunk=False)
def iri_ecmwf(start_time, end_time, variable, forecast_type,
              run_type="average", grid="global1_5", verbose=False):
    """Fetches forecast data from the ECMWF IRI dataset.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "reforecast".
        run_type (str): The type of run to fetch. One of:
            - average: to download the averaged of the perturbed runs
            - control: to download the control forecast
            - perturbed: to download all perturbed runs
            - [int 0-50]: to download a specific  perturbed run
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
        verbose (bool): Whether to print verbose output.
    """
    # Read and combine all the data into an array
    target_dates = get_dates(start_time, end_time,
                             stride="day", return_string=True)

    # Get correct single function
    fn = single_iri_ecmwf if forecast_type == "forecast" else single_iri_ecmwf_dense
    datasets = []
    for date in target_dates:
        ds = dask.delayed(fn)(
            date, variable, forecast_type, run_type, grid, verbose, filepath_only=True)
        datasets.append(ds)
    datasets = dask.compute(*datasets)
    data = [d for d in datasets if d is not None]
    if len(data) == 0:
        return None

    if forecast_type == "forecast":
        x = xr.open_mfdataset(data,
                              engine='zarr',
                              combine="by_coords",
                              parallel=True,
                              chunks={'lat': 121, 'lon': 240, 'lead_time': 46, 'start_date': 969})
        return x
    elif forecast_type == "reforecast":
        x = xr.open_mfdataset(data,
                              engine='zarr',
                              concat_dim='model_issuance_date',
                              combine="nested",
                              parallel=True,
                              chunks={'lat': 121, 'lon': 240, 'lead_time': 46, 'model_run': 1,
                                      'start_year': 20, 'model_issuance_date': 1})

        return x


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'forecast_type', 'grid'],
           timeseries=['start_date', 'model_issuance_date'],
           chunking={"lat": 121, "lon": 240, "lead_time": 46,
                     "start_date": 29, "start_year": 29,
                     "model_issuance_date": 1},
           auto_rechunk=False)
def ecmwf_averaged_iri(start_time, end_time, variable, forecast_type, grid="global1_5"):
    """Fetches forecast data from the ECMWF IRI dataset.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "hindcast".
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
    """
    if grid != 'global1_5':
        raise ValueError("Only the global1_5 grid is supported for ecmwf averaged.")

    # Ensure appropriate chunking for merge
    chunk_dict = {'lat': 121, 'lon': 240, 'lead_time': 46}
    if forecast_type == "reforecast":
        chunk_dict['start_year'] = 29
        chunk_dict['model_issuance_date'] = 1
    else:
        chunk_dict['start_date'] = 29

    # Read and combine all the data into an array
    df_control = iri_ecmwf(start_time, end_time, variable,
                           forecast_type, run_type="control",
                           grid=grid) \
        .rename({f"{variable}": f"{variable}_control"})
    # A note: this rechunking shouldn't be necessary, but it is
    df_control = df_control.chunk(chunk_dict)

    df_pert = iri_ecmwf(start_time, end_time, variable,
                        forecast_type, run_type="average",
                        grid=grid) \
        .rename({f"{variable}": f"{variable}_pert"})
    # A note: this rechunking shouldn't be necessary, but it is
    df_pert = df_pert.chunk(chunk_dict)

    # Combine control and perturbed runs
    if forecast_type == "forecast":
        M = 50.0  # forecast average is made up of 50 ensemble members
    else:
        M = 10.0  # reforecast average is made up of 10 ensemble members

    # Need to run a merge here, because sometimes pert and control
    # are available for different dates
    df = xr.merge([df_control, df_pert], join="outer")

    # Take a weighted average of the control and perturbed runs
    df[variable] = df[f"{variable}_control"] * 1./(M+1.) + df[f"{variable}_pert"] * M/(M+1.)
    df = df.drop([f"{variable}_control", f"{variable}_pert"])

    return df


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'forecast_type', 'grid'],
           timeseries=['start_date', 'model_issuance_date'],
           cache_disable_if={'grid': 'global1_5'},
           chunking={"lat": 121, "lon": 240, "lead_time": 46,
                     "start_date": 30,
                     "model_issuance_date": 30, "start_year": 1},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'model_issuance_date': 1}
               },
           },
           auto_rechunk=False)
def ecmwf_averaged_regrid(start_time, end_time, variable, forecast_type, grid='global1_5'):
    """IRI ECMWF average forecast with regridding."""
    ds = ecmwf_averaged_iri(start_time, end_time, variable, forecast_type, grid='global1_5')
    # Convert to base180 longitude
    ds = lon_base_change(ds, to_base="base180")

    if grid == 'global1_5':
        return ds
    # Regrid onto appropriate grid
    ds = regrid(ds, grid, base='base180', grid_chunks={"lat": 120, "lon": 120})
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'forecast_type', 'agg', 'grid'],
           timeseries=['start_date', 'model_issuance_date'],
           chunking={"lat": 121, "lon": 240, "lead_time": 46,
                     "start_date": 30,
                     "model_issuance_date": 30, "start_year": 1},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'model_issuance_date': 1}
               },
           },
           auto_rechunk=False)
def ecmwf_rolled(start_time, end_time, variable, forecast_type,
                 agg_days=14, grid="global1_5"):
    """Fetches forecast data from the ECMWF IRI dataset.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "hindcast".
        agg_days (int): The aggregation period, in days.
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
    """
    # Read and combine all the data into an array
    ds = ecmwf_averaged_regrid(start_time, end_time, variable, forecast_type, grid=grid)
    ds = ds.chunk({'lat': 721, 'lon': 1440})
    if forecast_type == "reforecast":
        ds = ds.chunk({'start_year': 29, 'model_issuance_date': 1})
    else:
        ds = ds.chunk({'start_date': 29})

    # Roll and aggregate the data
    ds = roll_and_agg(ds, agg=agg_days, agg_col="lead_time", agg_fn="mean")

    return ds
