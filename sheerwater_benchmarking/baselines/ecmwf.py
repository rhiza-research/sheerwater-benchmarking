"""Functions to fetch and process data from the ECMWF IRI dataset."""
import dask
import sys
import pandas as pd
from datetime import datetime
import os
import xarray as xr
import dateparser
from sheerwater_benchmarking.utils import dask_remote, cacheable

from sheerwater_benchmarking.utils.secrets import ecmwf_secret
from sheerwater_benchmarking.utils.general_utils import (get_grid, print_ok, print_info,
                                                         print_warning, print_error,
                                                         download_url, get_dates,
                                                         is_valid_forecast_date)

from sheerwater_benchmarking.data import land_sea_mask
from sheerwater_benchmarking.utils.data_utils import apply_mask, roll_and_agg


@dask_remote
@cacheable(data_type='array',
           cache_args=['time', 'variable', 'forecast_type', 'run_type', 'grid'],
           chunking={'lat': 121, 'lon': 240, 'lead_time': 46,
                     'start_date': 969, 'model_issuance_date': 1},
           auto_rechunk=False)
def single_iri_ecmwf(time, variable, forecast_type,
                     run_type="average", grid="global1_5",
                     verbose=True):
    """Fetches forecast data from the IRI ECMWF dataset.

    Args:
        time (str): The date to fetch data for (by day).
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "hindcast".
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
    single_model_run_url = f"M/({run_type})VALUES/" if type(
        run_type) is int else ""

    longitudes, latitudes, grid_size = get_grid(grid)
    restrict_lat_url = f"Y/{latitudes[0]}/{grid_size}/{latitudes[1]}/GRID/"
    restrict_lon_url = f"X/{longitudes[0]}/{grid_size}/{longitudes[1]}/GRID/"

    if variable == "tmp2m":
        # Convert from Kelvin to Celsius.
        differences_url = ""
        convert_units_url = "(Celsius_scale)/unitconvert/"
    else:
        differences_url = "[L]differences/"
        # convert_units_url = "(mm)/unitconvert/"
        convert_units_url = "c://name//water_density/def/998/(kg/m3)/:c/div/(mm)/unitconvert/"

    if verbose:
        print_ok("ecmwf", bold=True)

    date = dateparser.parse(time)
    day, month, year = datetime.strftime(date, "%d,%b,%Y").split(",")

    restrict_forecast_date_url = f"S/({day} {month} {year})VALUES/"

    if not is_valid_forecast_date("ecmwf", forecast_type, date):
        if verbose:
            print_warning(
                f"Skipping: {day} {month} {year} (not a valid {forecast_type} date).", skip_line_before=False,
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
        print_info(f"Downloading: {day} {month} {year}.", verbose=verbose)
        with open(file, "wb") as f:
            f.write(r.content)

        print_info(
            f"-done (downloaded {sys.getsizeof(r.content) / 1024:.2f} KB).\n",
            verbose=verbose,
        )
    elif r.status_code == 404:
        print_warning(bold=True, verbose=verbose)
        print_info(
            f"Data for {day} {month} {year} is not available for model ecmwf.\n", verbose=verbose)
        return None
    else:
        print_error(bold=True)
        print_info(
            f"Unknown error occured when trying to download data for {day} {month} {year} for model ecmwf.\n")
        return None

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
                print_warning(
                    f"No data found for: {day} {month} {year} (a valid {forecast_type} date).", skip_line_before=False,
                )
                return None

            # Reforecast-specific renaming
            rename_dict["hdate"] = "start_date"
            rename_dict["S"] = "model_issuance_date"
    except OSError:
        print_warning(
            f"Failed to load data for: {day} {month} {year} (a valid {forecast_type} date).", skip_line_before=False,
        )
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
           chunking={'lat': 121, 'lon': 240, 'lead_time': 46,
                     'start_year': 20, 'model_issuance_date': 1},
           auto_rechunk=False)
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
                     'start_year': 29, 'model_issuance_date': 1},
           auto_rechunk=False)
def iri_ecmwf(start_time, end_time, variable, forecast_type,
              run_type="average", grid="global1_5", verbose=False):
    """Fetches forecast data from the ECMWF IRI dataset.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "hindcast".
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

    # Get correct single funtion
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
                              chunks={'lat': 121, 'lon': 240, 'lead_time': 46,
                                      'start_year': 20, 'model_issuance_date': 1})

        return x


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'forecast_type', 'grid'],
           timeseries=['start_date', 'model_issuance_date'],
           chunking={"lat": 121, "lon": 240, "lead_time": 46,
                     "start_date": 969, "start_year": 29,
                     "model_issuance_date": 1},
           auto_rechunk=False)
def ecmwf_averaged(start_time, end_time, variable, forecast_type,
                   grid="global1_5", verbose=True):
    """Fetches forecast data from the ECMWF IRI dataset.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "hindcast".
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
        verbose (bool): Whether to print verbose output.
    """
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
                           grid=grid, verbose=verbose) \
        .rename({f"{variable}": f"{variable}_control"})
    # A note: this rechunking shouldn't be necessary, but it is
    df_control = df_control.chunk(chunk_dict)

    df_pert = iri_ecmwf(start_time, end_time, variable,
                        forecast_type, run_type="average",
                        grid=grid, verbose=verbose) \
        .rename({f"{variable}": f"{variable}_pert"})
    # A note: this rechunking shouldn't be necessary, but it is
    df_pert = df_pert.chunk(chunk_dict)

    # Combine control and perturbed runs
    if forecast_type == "forecast":
        M = 50.0  # forecast average is made up of 50 ensemble members
    else:
        M = 10.0  # reforecast average is made up of 10 ensemble members

    # Need to run a merge here, because sometimes pert and control
    # are available for differnt dates
    df = xr.merge([df_control, df_pert], join="outer")

    # Take a weigthed average of the control and perturbed runs
    df[variable] = df[f"{variable}_control"] * 1./(M+1.) + df[f"{variable}_pert"] * M/(M+1.)
    df = df.drop([f"{variable}_control", f"{variable}_pert"])

    return df


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'forecast_type', 'grid', 'agg'],
           timeseries=['start_date', 'model_issuance_date'],
           chunking={"lat": 32, "lon": 30, "lead_time": 1, "start_date": 969,
                     "start_year": 29, "model_issuance_date": 969},
           auto_rechunk=False)
def ecmwf_rolled(start_time, end_time, variable, forecast_type,
                 grid="global1_5", agg=14, verbose=True):
    """Fetches forecast data from the ECMWF IRI dataset.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "hindcast".
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
        agg (str): The aggregation period to use, in days
        verbose (bool): Whether to print verbose output.
    """
    # Read and combine all the data into an array
    ds = ecmwf_averaged(start_time, end_time, variable,
                        forecast_type,
                        grid=grid, verbose=verbose)

    # Roll and aggregate the data
    agg_fn = "sum" if variable == "precip" else "mean"
    ds = roll_and_agg(ds, agg=agg, agg_col="lead_time", agg_fn=agg_fn)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries=['start_date', 'model_issuance_date'],
           cache_args=['variable', 'forecast_type', 'grid', 'agg', 'mask'],
           chunking={"lat": 32, "lon": 30, "lead_time": 1, "start_date": 969,
                     "start_year": 29, "model_issuance_date": 969},
           auto_rechunk=False)
def ecmwf_agg(start_time, end_time, variable, forecast_type,
              grid="global1_5", agg=14, mask="lsm", verbose=True):
    """Fetches forecast data from the ECMWF IRI dataset.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        forecast_type (str): The type of forecast to fetch. One of "forecast" or "reforecast".
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
        agg (str): The aggregation period to use, in days
        mask (str): The mask to apply. One of:
            - lsm: land-sea mask
            - None: no mask
        verbose (bool): Whether to print verbose output.
    """
    ds = ecmwf_rolled(start_time, end_time, variable,
                      forecast_type, grid=grid, agg=agg,
                      verbose=verbose)

    if mask == "lsm":
        # Select varibles and apply mask
        mask_ds = land_sea_mask(grid=grid).compute()
    elif mask is None:
        mask_ds = None
    else:
        raise NotImplementedError("Only land-sea or None mask is implemented.")

    ds = apply_mask(ds, mask_ds, variable, val=0.0, rename_dict={"mask": variable})
    return ds
