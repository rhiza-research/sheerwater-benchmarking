"""Functions to fetch and process data from the ECMWF IRI dataset."""
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dateparser
import os
import xarray as xr
import requests
import ssl
from urllib3 import poolmanager
import time
import dask

from sheerwater_benchmarking.reanalysis import era5_rolled
from sheerwater_benchmarking.utils import (dask_remote, cacheable, ecmwf_secret,
                                           get_grid, get_dates,
                                           is_valid_forecast_date,
                                           roll_and_agg,
                                           apply_mask, clip_region,
                                           lon_base_change,
                                           regrid, get_variable)


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
                 agg=14, grid="global1_5"):
    """Fetches forecast data from the ECMWF IRI dataset.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "hindcast".
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
        agg (str): The aggregation period to use, in days
    """
    # Read and combine all the data into an array
    ds = ecmwf_averaged_regrid(start_time, end_time, variable, forecast_type, grid=grid)
    ds = ds.chunk({'lat': 721, 'lon': 1440})
    if forecast_type == "reforecast":
        ds = ds.chunk({'start_year': 29, 'model_issuance_date': 1})
    else:
        ds = ds.chunk({'start_date': 29})

    # Roll and aggregate the data
    agg_fn = "sum" if variable == "precip" else "mean"
    ds = roll_and_agg(ds, agg=agg, agg_col="lead_time", agg_fn=agg_fn)

    return ds


@dask_remote
@cacheable(data_type='array',
           cache=True,
           timeseries=['start_date', 'model_issuance_date'],
           cache_args=['variable', 'forecast_type', 'agg', 'grid', 'mask'],
           chunking={"lat": 32, "lon": 30, "lead_time": 1,
                     "start_date": 1000,
                     "model_issuance_date": 1000, "start_year": 29},
           auto_rechunk=False)
def ecmwf_agg(start_time, end_time, variable, forecast_type, agg=14, grid="global1_5",  mask="lsm"):
    """Fetches forecast data from the ECMWF IRI dataset.

    Specialized function for the ABC model

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "reforecast".
        agg (str): The aggregation period to use, in days
        grid (str): The grid resolution to fetch the data at.
        mask (str): The mask to apply. One of:
            - lsm: land-sea mask
            - None: no mask
        region (str): The region to clip the data to. One of:
            - global: global region
            - africa: the African continent
    """
    ds = ecmwf_rolled(start_time, end_time, variable,
                      forecast_type, agg=agg,  grid=grid)
    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'forecast_type', 'run_type', 'time_group', 'grid'],
           cache=False,
           timeseries=['start_date', 'model_issuance_date'],
           chunking={"lat": 121, "lon": 240, "lead_time": 46,
                     "start_date": 29, "start_year": 29,
                     "model_issuance_date": 1})
def ifs_extended_range_raw(start_time, end_time, variable, forecast_type,  # noqa ARG001
                           run_type='average', time_group='weekly', grid="global1_5"):
    """Fetches IFS extended range forecast data from the WeatherBench2 dataset.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "reforecast".
        run_type (str): The type of run to fetch. One of:
            - average: to download the averaged of the perturbed runs
            - perturbed: to download all perturbed runs
            - [int 0-50]: to download a specific  perturbed run
        time_group (str): The time grouping to use. One of: "daily", "weekly", "biweekly"
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
    """
    if grid != 'global1_5':
        raise NotImplementedError("Only global 1.5 degree grid is implemented.")

    forecast_str = "-reforecast" if forecast_type == "reforecast" else ""
    run_str = "_ens_mean" if run_type == "average" else ""
    avg_str = "-avg" if time_group == "daily" else "_avg"  # different naming for daily
    time_str = "" if time_group == "daily" else "-weekly" if time_group == "weekly" else "-biweekly"
    file_str = f'ifs-ext{forecast_str}-full-single-level{time_str}{avg_str}{run_str}.zarr'
    filepath = f'gs://weatherbench2/datasets/ifs_extended_range/{time_group}/{file_str}'

    # Pull the google dataset
    ds = xr.open_zarr(filepath)

    # Select the right variable
    var = get_variable(variable, 'ecmwf_ifs_er')
    ds = ds[var].to_dataset()
    ds = ds.rename_vars(name_dict={var: variable})

    # Convert local dataset naming and units
    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon', 'prediction_timedelta': 'lead_time'})
    if run_type != 'average':
        ds = ds.rename({'number': 'member'})
    if forecast_type == 'reforecast':
        ds = ds.rename({'hindcast_year': 'start_year'})
        ds = ds.rename({'forecast_time': 'model_issuance_date'})
        ds = ds.drop('time')
    else:
        ds = ds.rename({'time': 'start_date'})

    ds = ds.drop('valid_time')

    # If a specific run, select
    if isinstance(run_type, int):
        ds = ds.sel(member=run_type)
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'forecast_type', 'run_type', 'time_group', 'grid'],
           cache=True,
           timeseries=['start_date', 'model_issuance_date'],
           chunking={"lat": 721, "lon": 1440, "lead_time": 1,
                     "start_date": 200,
                     "model_issuance_date": 200, "start_year": 1,
                     "member": 1},
           auto_rechunk=False)
def ifs_extended_range(start_time, end_time, variable, forecast_type,
                       run_type='average', time_group='weekly', grid="global1_5"):
    """Fetches IFS extended range forecast and reforecast data from the WeatherBench2 dataset.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "reforecast".
        run_type (str): The type of run to fetch. One of:
            - average: to download the averaged of the perturbed runs
            - perturbed: to download all perturbed runs
            - [int 0-50]: to download a specific  perturbed run
        time_group (str): The time grouping to use. One of: "daily", "weekly", "biweekly"
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
    """
    """IRI ECMWF average forecast with regridding."""
    ds = ifs_extended_range_raw(start_time, end_time, variable, forecast_type,
                                run_type, time_group, grid='global1_5')
    # Convert to base180 longitude
    ds = lon_base_change(ds, to_base="base180")

    if variable == 'tmp2m':
        ds[variable] = ds[variable] - 273.15
        ds.attrs.update(units='C')
    elif variable == 'precip':
        ds[variable] = ds[variable] * 1000.0
        ds.attrs.update(units='mm')
        ds = np.maximum(ds, 0)
        if time_group == 'weekly':
            ds = ds * 7
        elif time_group == 'biweekly':
            ds = ds * 14
        else:
            raise ValueError("Only weekly and biweekly aggregations are supported for precipitation.")

    if grid == 'global1_5':
        return ds
    # Regrid onto appropriate grid
    if forecast_type == 'reforecast':
        raise NotImplementedError("Regridding reforecast data should be done with extreme care. It's big.")

    # Need all lats / lons in a single chunk to be reasonable
    chunks = {'lat': 121, 'lon': 240, 'lead_time': 1}
    if run_type == 'perturbed':
        chunks['member'] = 1
        chunks['start_date'] = 200
    else:
        chunks['start_date'] = 200
    ds = ds.chunk(chunks)
    method = 'conservative' if variable == 'precip' else 'linear'
    # Need all lats / lons in a single chunk for the output to be reasonable
    ds = regrid(ds, grid, base='base180', method=method, time_dim="start_date",
                output_chunks={"lat": 721, "lon": 1440})
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def ecmwf_ifs_er(start_time, end_time, variable, lead, prob_type='deterministic',
                 grid='global1_5', mask='lsm', region="global"):
    """Standard format forecast data for ECMWF forecasts."""
    lead_params = {
        "week1": ('weekly', 0),
        "week2": ('weekly', 7),
        "week3": ('weekly', 14),
        "week4": ('weekly', 21),
        "week5": ('weekly', 28),
        "week6": ('weekly', 35),
        "weeks12": ('biweekly', 0),
        "weeks23": ('biweekly', 7),
        "weeks34": ('biweekly', 14),
        "weeks45": ('biweekly', 21),
        "weeks56": ('biweekly', 28),
    }
    time_group, lead_id = lead_params.get(lead, (None, None))
    if time_group is None:
        raise NotImplementedError(f"Lead {lead} not implemented for ECMWF forecasts.")

    if prob_type == 'deterministic':
        ds = ifs_extended_range(start_time, end_time, variable, forecast_type="forecast",
                                run_type='average', time_group=time_group, grid=grid)
        ds = ds.assign_attrs(prob_type="deterministic")
    else:  # probabilistic
        ds = ifs_extended_range(start_time, end_time, variable, forecast_type="forecast",
                                run_type='perturbed', time_group=time_group, grid=grid)
        ds = ds.assign_attrs(prob_type="ensemble")

    # Get specific lead
    ds = ds.sel(lead_time=np.timedelta64(lead_id, 'D'))
    ds = ds.rename({'start_date': 'time'})

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'lead', 'run_type', 'time_group', 'grid'],
           timeseries=['model_issuance_date'],
           cache=True,
           chunking={"lat": 121, "lon": 240, "lead_time": 1, "model_issuance_date": 1000})
def ifs_er_reforecast_lead_bias(start_time, end_time, variable, lead=0, run_type='average',
                                time_group='weekly', grid="global1_5"):
    """Computes the bias of ECMWF reforecasts for a specific lead."""
    # Fetch the reforecast data; get's the past 20 years associated with each start date
    ds_deb = ifs_extended_range(start_time, end_time, variable, forecast_type="reforecast",
                                run_type=run_type, time_group=time_group, grid=grid)

    # Get the appropriate lead
    n_leads = len(ds_deb.lead_time)
    if lead >= n_leads:
        # Lead does not exist
        return None
    ds_deb = ds_deb.sel(lead_time=np.timedelta64(lead, 'D'))

    # We need ERA5 data from the start_time to 20 years before the first date
    first_date = ds_deb.model_issuance_date.min().values
    last_date = ds_deb.model_issuance_date.max().values
    new_start = (first_date.astype('M8[D]').astype('O') - relativedelta(years=20)).strftime("%Y-%m-%d")
    # We need ERA5 data from the end time to the end time plus last lead in days
    new_end = (
        (last_date + ds_deb.lead_time.values).astype('M8[D]').astype('O') - relativedelta(years=1)).strftime("%Y-%m-%d")

    # Get the pre-aggregated ERA5 data
    agg = {'daily': 1, 'weekly': 7, 'biweekly': 14}[time_group]
    ds_truth = era5_rolled(new_start, new_end, variable, agg=agg, grid=grid)

    def get_bias(ds_sub):
        """Get the 20-year estimated bias of the reforecast data."""
        # The the corresponding forecast dates for the reforecast data
        dates = [np.datetime64((ds_sub['model_issuance_date'].values[0].astype('M8[D]').astype('O')
                                + relativedelta(years=x)))
                 for x in ds_sub.start_year]

        # Adjust each forecast date by the lead time (0, 1, 2, ... days)
        lead_td = ds_deb.lead_time.values
        lead_dates = [x + lead_td for x in dates]

        # Select the subset of the ground truth matching this lead
        ds_truth_lead = ds_truth.sel(time=lead_dates)

        # # Assign the time coordinate to match the forecast dataframe for alignment and subtract
        ds_truth_lead = ds_truth_lead.assign_coords(time=ds_sub.start_year.values).rename(time='start_year')
        bias = (ds_truth_lead - ds_sub).mean(dim='start_year')
        return bias

    bias = ds_deb.groupby('model_issuance_date').map(get_bias)
    return bias


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'run_type', 'time_group', 'grid'],
           timeseries=['model_issuance_date'],
           cache=True,
           chunking={"lat": 121, "lon": 240, "lead_time": 1, "model_issuance_date": 1000})
def ifs_er_reforecast_bias(start_time, end_time, variable, run_type='average', time_group='weekly', grid="global1_5"):
    """Computes the bias of ECMWF reforecasts for all leads."""
    # Fetch the reforecast data to calculate how many leads we need
    if time_group == 'weekly':
        leads = [0, 7, 14, 21, 28, 35]
    else:
        leads = [0, 7, 14, 21, 28]

    # Accumulate all the per lead biases
    biases = []
    for i in leads:
        biases.append(ifs_er_reforecast_lead_bias(start_time, end_time, variable, lead=i,
                                                  run_type=run_type, time_group=time_group, grid=grid))
    # Concatenate leads and unstack
    ds_biases = xr.concat(biases, dim='lead_time')
    return ds_biases


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'margin_in_days', 'run_type', 'time_group', 'grid'],
           cache=True,
           timeseries=['start_date'],
           chunking={"lat": 121, "lon": 240, "lead_time": 1, 'start_date': 1000, "member": 1})
def ifs_extended_range_debiased(start_time, end_time, variable, margin_in_days=6,
                                run_type='average', time_group='weekly', grid="global1_5"):
    """Computes the debiased ECMWF forecasts."""
    # Get bias data from reforecast; for now, debias with deterministic bias
    ds_b = ifs_er_reforecast_bias(start_time, end_time, variable,
                                  run_type='average', time_group=time_group, grid=grid)

    # Get forecast data
    ds_f = ifs_extended_range(start_time, end_time, variable, forecast_type='forecast',
                              run_type=run_type, time_group=time_group, grid=grid)
    if time_group == 'weekly':
        leads = [np.timedelta64(x, 'D') for x in [0, 7, 14, 21, 28, 35]]
    else:
        leads = [np.timedelta64(x, 'D') for x in [0, 7, 14, 21, 28]]
    ds_f = ds_f.sel(lead_time=leads)

    def bias_correct(ds_sub, margin_in_days=6):
        date = ds_sub.start_date.values
        dt = np.timedelta64(margin_in_days, 'D')
        nbhd = (ds_b.model_issuance_date.values >= date - dt) & \
            (ds_b.model_issuance_date.values <= date + dt)
        if nbhd.sum() == 0:  # No data to debias
            raise ValueError('No debiasing data found.')

        nbhd_df = ds_b.isel(model_issuance_date=nbhd).mean(dim='model_issuance_date')
        dsp = ds_sub + nbhd_df
        return dsp

    ds = ds_f.groupby('start_date').map(bias_correct, margin_in_days=margin_in_days)
    # Should not be below zero after bias correction
    if variable == 'precip':
        ds = np.maximum(ds, 0)
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'margin_in_days', 'run_type', 'time_group', 'grid'],
           cache=True,
           timeseries=['start_date'],
           chunking={"lat": 721, "lon": 1440, "lead_time": 1,
                     "start_date": 200,
                     "model_issuance_date": 200, "start_year": 1,
                     "member": 1})
def ifs_extended_range_debiased_regrid(start_time, end_time, variable, margin_in_days=6,
                                       run_type='average', time_group='weekly', grid="global1_5"):
    """Computes the debiased ECMWF forecasts."""
    ds = ifs_extended_range_debiased(start_time, end_time, variable, margin_in_days=margin_in_days,
                                     run_type=run_type, time_group=time_group, grid='global1_5')
    if grid == 'global1_5':
        return ds

    # Need all lats / lons in a single chunk to be reasonable
    chunks = {'lat': 121, 'lon': 240, 'lead_time': 1}
    if run_type == 'perturbed':
        chunks['member'] = 1
        chunks['start_date'] = 200
    else:
        chunks['start_date'] = 200
    ds = ds.chunk(chunks)
    method = 'conservative' if variable == 'precip' else 'linear'
    # Need all lats / lons in a single chunk for the output to be reasonable
    ds = regrid(ds, grid, base='base180', method=method, time_dim="start_date",
                output_chunks={"lat": 721, "lon": 1440})
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def ecmwf_ifs_er_debiased(start_time, end_time, variable, lead, prob_type='deterministic',
                          grid='global1_5', mask='lsm', region="global"):
    """Standard format forecast data for ECMWF forecasts."""
    lead_params = {
        "week1": ('weekly', 0),
        "week2": ('weekly', 7),
        "week3": ('weekly', 14),
        "week4": ('weekly', 21),
        "week5": ('weekly', 28),
        "week6": ('weekly', 35),
        "weeks12": ('biweekly', 0),
        "weeks23": ('biweekly', 7),
        "weeks34": ('biweekly', 14),
        "weeks45": ('biweekly', 21),
        "weeks56": ('biweekly', 28),
    }
    time_group, lead_id = lead_params.get(lead, (None, None))
    if time_group is None:
        raise NotImplementedError(f"Lead {lead} not implemented for ECMWF debiased forecasts.")

    if prob_type == 'deterministic':
        ds = ifs_extended_range_debiased_regrid(start_time, end_time, variable, margin_in_days=6,
                                                run_type='average', time_group=time_group, grid=grid)
        ds = ds.assign_attrs(prob_type="deterministic")
    else:  # probabilistic
        ds = ifs_extended_range_debiased_regrid(start_time, end_time, variable, margin_in_days=6,
                                                run_type='perturbed', time_group=time_group, grid=grid)
        ds = ds.assign_attrs(prob_type="ensemble")

    # Get specific lead
    ds = ds.sel(lead_time=np.timedelta64(lead_id, 'D'))
    ds = ds.rename({'start_date': 'time'})

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)
    # Clip to specified region
    ds = clip_region(ds, region=region)
    return ds
