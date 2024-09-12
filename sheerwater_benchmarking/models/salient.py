"""Pulls Saleint Predictions S2S forecasts from the Salient API."""
import xarray as xr
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import sys
import os

import salientsdk as sk

from sheerwater_benchmarking.utils import dask_remote, cacheable, salient_auth
from sheerwater_benchmarking.utils.general_utils import get_dates
from sheerwater_benchmarking.utils.model_utils import get_salient_loc


@salient_auth
@dask_remote
@cacheable(data_type='array',
           timeseries=['time'],
           cache_args=['variable', 'forecast_type', 'grid', 'agg', 'mask'],
           chunking={"lat": 32, "lon": 30, "lead_time": 1, "start_date": 969,
                     "start_year": 29, "model_issuance_date": 969},
           auto_rechunk=False)
def salient_forecast(start_time, end_time, variable, forecast_type,
                     timescale, value_type="vals",
                     grid="global1_5", mask="lsm", verbose=True):
    """Fetch salient data from the Salient API.

    Args:   
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch data for.
        variable (str): The variable to fetch.
        forecast_type (str): The type of forecast to fetch.
        timescale (str): The timescale of the forecast. One of:
            - sub-seasonal 
            - seasonal
            - long-range
        grid (str): The grid resolution to fetch the data at.
        mask (str): The mask to apply to the data.
        verbose (bool): Whether to print verbose output. 
    """
    # Write data to local temporary file
    sk.set_file_destination("./temp")

    # Get the Salient locaiton file
    loc = get_salient_loc(grid) 

    # date_range = pd.date_range(start=start_time, end=end_time, freq="W").strftime("%Y-%m-%d").tolist()
    target_dates = get_dates(start_time, end_time, "weekly", return_string=True)

    ds = sk.forecast_timeseries(
        loc=loc,
        variable=variable,
        field=value_type,
        date=target_dates,  # request multiple forecast dates
        timescale=timescale,
        model="blend",
        verbose=True,
        force=False,
        strict=False,  # there is missing data in 2020.  Work around it.
    )

    ds = xr.load_dataset(ds["file_name"].values[0])
    import pdb
    pdb.set_trace()
    return ds
